#include "MySQLDriver.h"
#include "MySQLFactory.h"

using namespace server::mysqldb;

/* ResultSet */
ResultSet::ResultSet()
: row_(NULL), affected_rows_(0), lastid_(0), columns_(0) {

}

ResultSet::ResultSet(boost::shared_ptr<MYSQL_RES> result, uint32_t affected_rows, uint64_t lastid) 
: result_(result), row_(NULL), affected_rows_(affected_rows), lastid_(lastid) {

	if (result.get() != NULL)
	{
		columns_ = mysql_num_fields(result.get());
	}
	else
	{
		columns_ = 0;
	}
}

ResultSet::~ResultSet() {
}

Column ResultSet::get(int index) const {
	if (row_ == NULL)
		throw Exception(-1, "End of result set");

	unsigned int columns = mysql_num_fields(result_.get());
	if (index == 0 || index > (int)columns) {
		throw Exception(-1, "Invalid field index: %d", index);
	}
	return Column(row_[index - 1], lengths_[index - 1]);
}

Column ResultSet::get(const char *name) const {
	if (row_ == NULL)
		throw Exception(-1, "End of result set");

	MYSQL_FIELD *fields = mysql_fetch_fields(result_.get());
	unsigned int columns = mysql_num_fields(result_.get());

	for (unsigned int i = 0; i != columns; ++i) {
		if (strcmp(fields[i].name, name) == 0) {
			return Column(row_[i], lengths_[i]);
		}
	}

	throw Exception(-1, "Invalid field name: %s", name);
}

MYSQL_FIELD* ResultSet::getFields()
{
	if (result_.get() == NULL)
		return NULL;

	return mysql_fetch_field(result_.get()) ;
		
}
bool ResultSet::next() {
	if (result_.get() == NULL)
		return false;

	if ((row_ = mysql_fetch_row(result_.get())) != NULL) {
		lengths_ = mysql_fetch_lengths(result_.get());
		return true;
	} else {
		return false;
	}
}

long long ResultSet::getInt(int index, long long df /* =0 */) const {
	return get(index).toInt(df);
}

long long ResultSet::getInt(const char *name, long long df /* =0 */) const {
	return get(name).toInt(df);
}

std::string ResultSet::getString(int index) const {
	return get(index).toString();
}

std::string ResultSet::getString(const char *name) const {
	return get(name).toString();
}

/* Connection */
Connection::Connection(const std::string &user, 
                       const std::string &passwd, 
                       const std::string &database, 
                       const std::string &host, 
                       unsigned short port,
                       unsigned int connect_timeout,
                       unsigned int read_timeout,
                       const std::string& charset,
                       bool autocommit) : connected_(false) {
#ifdef LINUX
	mysql_thread_init();
#endif

	user_ = user;
	passwd_ = passwd;
	database_ = database;
	host_ = host;
	port_ = port;
	autocommit_ = autocommit ;
    connect_timeout_ = (connect_timeout/3 > 0) ? connect_timeout/3 : 1; //mysql driver will retry 3 times.
    read_timeout_ = (read_timeout/3 > 0)? read_timeout/3 : 1;
	if ( charset.empty() )
		charset_ = "utf8" ;
	else
		charset_ = charset ;
}

Connection::~Connection() {
	disconnect();
#ifdef LINUX
	mysql_thread_end();
#endif
}

void Connection::connect(){
	if (connected_) {
		return;
	}

	assert(mysql_init(&mysql_) != NULL);
    mysql_options(&mysql_, MYSQL_OPT_READ_TIMEOUT, &read_timeout_);
    mysql_options(&mysql_, MYSQL_OPT_CONNECT_TIMEOUT , &connect_timeout_);
	if (mysql_real_connect(&mysql_, host_.c_str(), user_.c_str(), passwd_.c_str(), database_.c_str(), port_, NULL, 0) != &mysql_) {
		throw Exception(&mysql_);
	}

	mysql_autocommit(&mysql_,autocommit_) ;
	std::string strSQL = "set names "  ;
	strSQL += charset_ ;
	int rc = mysql_query(&mysql_,strSQL.c_str()) ;
	if ( rc )
	{
		throw Exception(&mysql_) ;
	}
	connected_ = true;
}

void Connection::reconnect() {
	disconnect();
	connect();
}

Statement Connection::createStatement() {
	return Statement(&mysql_);
}

void Connection::disconnect() { 
	if (connected_) {
		mysql_close(&mysql_);
		connected_ = false;
	}
}

void Connection::close() {
    factory_->releaseConnection(database_, this);  
}

void Connection::begin() {
	if (mysql_query(&mysql_, "begin") != 0) {
		throw Exception(&mysql_);
	}
}

void Connection::commit() {
	if (mysql_commit(&mysql_) != 0) {
		throw Exception(&mysql_);
	}
}

void Connection::rollback() {
	if (mysql_rollback(&mysql_) != 0) {
		throw Exception(&mysql_);
	}
}

void Connection::setAutoCommit(bool b) {
	my_bool ret = mysql_autocommit(&mysql_, b ? 1 : 0);

	if (!ret) {
		throw Exception(&mysql_);
	}
}

void Connection::setCharSet(const std::string &name) {
	if (mysql_set_character_set(&mysql_, name.c_str()) != 0) {
		throw Exception(&mysql_);
	}
	//std::string sql = "set names " + name;
	//return query(sql).status();
}

/* Statement */
Statement::Statement(MYSQL *mysql): mysql_(mysql) {
}

Statement::~Statement() {
}

void Statement::prepare(const std::string &sql) {
	sql_ = sql;
}

ResultSet Statement::execute() {
	if (mysql_real_query(mysql_, sql_.data(), sql_.size()) != 0) {
		throw Exception(mysql_);
	}
	
	MYSQL_RES *result = mysql_store_result(mysql_);
	uint32_t affected_row = mysql_affected_rows(mysql_);

	boost::shared_ptr<MYSQL_RES> pResult(result, FreeMySQLResult());

	return ResultSet(pResult, affected_row, mysql_insert_id(mysql_) );
}

static std::string join(const std::vector<int64_t> &vec, const char *c) {
	char buf[23];
	std::string out;

	for (std::vector<int64_t>::size_type i = 0, j = vec.size(); i < j; ++i) {
#if __WORDSIZE == 64
		int len = snprintf(buf, sizeof(buf), "%ld", vec[i]);
#else
		int len = snprintf(buf, sizeof(buf), "%lld", vec[i]);
#endif
		out.append(buf, len);
		if (i + 1 < vec.size())
			out.append(c);
	}

	return out;
}

static std::string::size_type replace(std::string &sql, const char *from, const std::string &to, std::string::size_type pos) {
	sql.erase(pos, strlen(from));
	sql.insert(pos, to);
	return pos + to.size();
}

std::string::size_type Statement::bindInt(std::string::size_type pos, const char *from, int64_t i) {
	std::string to;
	to.resize(23);

	char *buf = (char *) to.data();
#if __WORDSIZE == 64
	int len = snprintf(buf, 23, "%ld", i);
#else
	int len = snprintf(buf, 23, "%lld", i);
#endif
	to.resize(len);
	return replace(sql_, from, to, pos);
}

std::string Statement::escape(const std::string &fr) {
	std::string to;
	if (fr.empty()) {
		to.assign("''");
	} else {
		to.resize(fr.size() * 2 + 3);
		char *buf = (char *) to.data();
		buf[0] = '\'';
		int len = mysql_real_escape_string(mysql_, buf + 1, fr.data(), fr.size());
		buf[len + 1] = '\'';
		to.resize(len + 2);
	}

	return to;
}

std::string::size_type Statement::bindString(std::string::size_type pos, const char *from, const char *data, size_t size) {
	std::string to;

	if (size == 0) {
		to.assign("''");
	} else {      
		to.resize(size * 2 + 3);
		char *buf = (char *) to.data();
		buf[0] = '\'';
		int len = mysql_real_escape_string(mysql_, buf + 1, data, size);
		buf[len + 1] = '\'';
		to.resize(len + 2);
	}             
	return replace(sql_, from, to, pos);
}

void Statement::bindParams(const std::vector<Parameter> &param) {
	int idx;
	char from[4];
	std::string::size_type pos = 0;

	do {
		pos = sql_.find(":", pos);
		if (pos == std::string::npos)
			break;

		idx = atoi(sql_.c_str() + pos + 1);
		assert(idx != 0 && (int)param.size() >= idx);
		snprintf(from, sizeof(from), ":%d", idx);
		const Parameter &p = param[idx - 1];

		switch (p.type) {
			case Parameter::INTEGER:
				{
					pos = bindInt(pos, from, p.data.integer);
					break;
				}
			case Parameter::STRING:
				{
					pos = bindString(pos, from, p.data.string, strlen(p.data.string));
					break;
				}
			case Parameter::BLOB:
				{
					pos = bindString(pos, from, p.data.blob->data(), p.data.blob->size());
					break;
				}
			case Parameter::INT_VECTOR:
				{
					std::string vec = join(*(p.data.int_vec), ",");
					pos = replace(sql_, from, vec, pos);
					break;
				}
			default:
				{
					break;
				}
		}
	} while (pos < sql_.size());
}
