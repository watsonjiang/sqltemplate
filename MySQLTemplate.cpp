#include "MySQLTemplate.h"

namespace server {
namespace mysqldb {


static int executeSQL(Callback *callback, bool preview, Connection *conn, const char *sql, const std::vector<Parameter> *param) {
	if (conn == NULL) {
		if (callback) {
			callback->onException(Exception(-1, "get connection failed"));
		}
		return 2006;
	}

	//uint64_t _lastCycle1 = measure_metrics::get_cpu_cycle();
	//uint64_t _checkCycle = 1000 * measure_metrics::getCpuFreq();

	Statement stmt = conn->createStatement();
	stmt.prepare(sql);

	if (param != NULL) {
		stmt.bindParams(*param);
	}
	if (preview) {
		if (callback) {
			callback->onPreview(stmt.preview());
		}
	}

	try {
		ResultSet result = stmt.execute();
		if (callback) {
			callback->onResult(result);
		}
	} catch (Exception &e) {
		if (callback)
			callback->onException(e);
		return e.code();
	}

	//uint64_t _lastCycle2 = measure_metrics::get_cpu_cycle();
	//uint64_t _useTime = (_lastCycle2-_lastCycle1) / _checkCycle;
	//if (_useTime > 200)//200ms
	//{
//		YY_LOG_INFO( "thread:%d usetime:%d %s", CLinuxSysTools::gettid(), _useTime, stmt.preview().c_str());
//	}

	return 0;
}

/* MySQLTemplate */
MySQLTransaction MySQLTemplate::beginTransaction() {
	MySQLTransaction tx(server::mysqldb::MYSQL_FACTORY::instance().getConnection(dbname_));
	tx.setPreview(preview());
	tx.begin();
	return tx;
}

bool MySQLTemplate::execSQL(Callback *callback, const char *sql, const std::vector<Parameter> *param) {
	static int max_reconnect = 2;

	for (int i = 0; i < max_reconnect; ++i) {
		Connection* conn = server::mysqldb::MYSQL_FACTORY::instance().getConnection(dbname_);

		int err = executeSQL(callback, preview(), conn, sql, param);
		if (err == 0) {
			if ( !conn->autocommit() )
				conn->commit() ;
            conn->release();
			return true;
		} else if ( err >=2000 && err <= 2018) {
			//Fatal error, unrecoverable
			if ( conn ) 
				conn->disconnect();
                conn->release(); 
		} else {
			if ( !conn->autocommit() )
				conn->rollback() ;
            conn->release();
			break;
		}
	}

	return false;
}

/* MySQLTransaction */
bool MySQLTransaction::begin() {

	if (conn_ == NULL)
		return false;

	try {
		conn_->begin();
	} catch (Exception &e) {
		//YY_LOG_ERROR( "thread[%d] begin transaction failed: %s", CLinuxSysTools::gettid(), e.what());
		if (e.code() <= 2018)
			conn_->disconnect();
		return false;
	}

	if (preview()) {
		//YY_LOG_DEBUG( "thread[%d] begin", CLinuxSysTools::gettid());
	}
	return true;
}

bool MySQLTransaction::execSQL(Callback *callback, const char *sql, const std::vector<Parameter> *args) {
	int err = executeSQL(callback, preview(), conn_, sql, args);
	if (err == 0) {		
		return true;
	} else if (err <= 2018) {
		//Fatal error, unrecoverable
		conn_->disconnect();
	}
	conn_ = NULL;
	return false;
}


bool MySQLTransaction::commit() {
	if (conn_ == NULL)
		return false;
	try {
		conn_->commit();
	} catch (Exception &e) {
		//YY_LOG_ERROR( "thread[%d] commit failed: %s", CLinuxSysTools::gettid(), e.what());
		if (e.code() <= 2018)
			conn_->disconnect();
		return false;
	}

	if (preview()) {
		//YY_LOG_DEBUG( "thread[%d] commit", CLinuxSysTools::gettid());
	}
	return true;
}

void MySQLTransaction::rollback() {
	if (conn_ == NULL)
		return;
	conn_->rollback();
}

}	//mysqldb
}	//server
