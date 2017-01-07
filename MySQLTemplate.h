#ifndef __YY_MYSQLLIB_TEMPLATE_H__
#define __YY_MYSQLLIB_TEMPLATE_H__

#include "MySQLFactory.h"
#include <vector>

namespace server {
namespace mysqldb {

struct Callback {
	virtual void onPreview(const std::string &sql) {}

	virtual void onResult(ResultSet &result) {}

	virtual void onException(const Exception &ex) {}
};	//Callback

struct NOPCallback : public Callback
{
	int16_t error_;
	std::string errorMsg_;

	virtual void onException(const Exception &ex)
	{
		error_ = ex.code();
		errorMsg_ = ex.what();
	}
};

struct SingleResultSet : public Callback
{
	virtual void onResult(ResultSet &result)
	{
		result_.clear();

		if (result.next())
		{
			uint32_t columns = result.getColumns();
			for(uint32_t pos = 0; pos < columns; ++pos)
			{
				/// index begin with 1
				result_.push_back(result.getString(pos+1));
			}
		}
	}

	virtual void onException(const Exception &ex)
	{
		error_ = ex.code();
		errorMsg_ = ex.what();
	}

	std::vector<std::string> result_ ;
	int16_t error_ ;
	std::string errorMsg_ ;
};

struct SingleVect : public Callback
{
	void onResult(ResultSet &result)
	{
		result_.clear();

		while (result.next())
		{
			result_.push_back(result.getString(1));
		}
	}

	void onException(const Exception &ex)
	{
		error_ = ex.code();
		errorMsg_ = ex.what(); 
	}

	std::vector<std::string> result_ ;
	int16_t error_ ;
	std::string errorMsg_ ;
};

struct MultiResultSet : public Callback
{
	virtual void onResult(ResultSet &result)
	{
		result_.clear();

		uint32_t columns = result.getColumns();
		while(result.next())
		{
			std::vector<std::string> row;
			for(uint32_t pos=0; pos<columns; ++pos)
			{
				row.push_back(result.getString(pos+1));
			}	
			result_.push_back(row);
		}
	}

	virtual void onException(const Exception &ex) 
	{
		error_ = ex.code();    
		errorMsg_ = ex.what(); 
	}

	std::vector<std::vector<std::string> > result_ ;
	int16_t error_ ;       
	std::string errorMsg_ ;
};

struct RealResultSet : public Callback
{
	virtual void onResult(ResultSet &result)
	{
		result_ = result; 
	}
	virtual void onException(const Exception &ex) 
	{
		error_ = ex.code();    
		errorMsg_ = ex.what(); 
	}
	
	ResultSet result_;
	int16_t error_ ;       
	std::string errorMsg_ ;
};

class SQLTemplate {
public:
	SQLTemplate(): preview_(false) {}

	virtual ~SQLTemplate() {}

	bool execute(Callback *callback, const char *sql) {
		return execSQL(callback, sql, NULL);
	}

	template<typename Arg1>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2, typename Arg3>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2, const Arg3 &a3) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));
		param.push_back(Parameter(a3));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2, typename Arg3, typename Arg4>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2, const Arg3 &a3, const Arg4 &a4) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));
		param.push_back(Parameter(a3));
		param.push_back(Parameter(a4));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2, const Arg3 &a3, const Arg4 &a4, const Arg5 &a5) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));
		param.push_back(Parameter(a3));
		param.push_back(Parameter(a4));
		param.push_back(Parameter(a5));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2, const Arg3 &a3, const Arg4 &a4, const Arg5 &a5, const Arg6 &a6) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));
		param.push_back(Parameter(a3));
		param.push_back(Parameter(a4));
		param.push_back(Parameter(a5));
		param.push_back(Parameter(a6));

		return execSQL(callback, sql, &param);
	}

	template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5, typename Arg6, typename Arg7>
	bool execute(Callback *callback, const char *sql, const Arg1 &a1, const Arg2 &a2, const Arg3 &a3, const Arg4 &a4, const Arg5 &a5, const Arg6 &a6, const Arg7 &a7) {
		std::vector<Parameter> param;
		param.push_back(Parameter(a1));
		param.push_back(Parameter(a2));
		param.push_back(Parameter(a3));
		param.push_back(Parameter(a4));
		param.push_back(Parameter(a5));
		param.push_back(Parameter(a6));
		param.push_back(Parameter(a7));

		return execSQL(callback, sql, &param);
	}

	virtual bool execSQL(Callback *callback, const char *sql, const std::vector<Parameter> *args) = 0;

	bool preview() { return preview_; }

	void setPreview(bool yes) { preview_ = yes; }

private:
	bool preview_;
};

class MySQLTransaction;

class MySQLTemplate: public SQLTemplate {
public:
	MySQLTemplate(const std::string &dbname): dbname_(dbname) {}

	virtual ~MySQLTemplate() {}

	MySQLTransaction beginTransaction();

	bool execSQL(Callback *callback, const char *sql, const std::vector<Parameter> *args);

private:	
	std::string dbname_;
};	//MySQLTemplate

class MySQLTransaction: public SQLTemplate {
public:
	explicit MySQLTransaction(Connection *conn): conn_(conn) {}

	virtual ~MySQLTransaction() {}

	bool active() { return (conn_ != NULL && conn_->connected()); }

	bool begin();

	bool execSQL(Callback *callback, const char *sql, const std::vector<Parameter> *args);

	bool commit();

	void rollback();

private:
	Connection *conn_;
};	//MySQLTransaction

}
}
#endif //__YY_MYSQLLIB_TEMPLATE_H__
