#ifndef MYSQLLIB_DRIVER_H
#define MYSQLLIB_DRIVER_H

#include <unistd.h>
#include <mysql.h>
#include <errmsg.h>
#include <string>
#include <vector>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <boost/shared_ptr.hpp>

namespace server {
	namespace mysqldb {

		class Column {
		public:
			explicit Column(const char *d, unsigned int s): data_(d), size_(s) {}

			~Column() {}

			inline const char *data() const { return data_; }

			inline unsigned int size() const { return size_; }

			std::string toString() const { if ( data_ == NULL ) return ""; else return std::string(data_, size_); }

			long long toInt(long long df) const { return null()?df:atoll(data_); }

			bool null() const { return data_ == NULL; }

		private:
			const char *data_;
			unsigned int size_;
		};	//Column

		class Exception {
		public:
			explicit Exception(int code, const char *fmt, ...): code_(code) {
				va_list va;
				va_start(va, fmt);
				vsnprintf(msg_, sizeof(msg_), fmt, va);
				va_end(va);
			}

			explicit Exception(MYSQL *mysql)
			{
				code_ = mysql_errno(mysql);
				memset(msg_,0,sizeof(msg_));
				strncpy(msg_, mysql_error(mysql),sizeof(msg_)-1);
			}

			~Exception() {}

			inline int code() const { return code_; }

			inline const char *what() const { return msg_; }
		private:
			int code_;
			char msg_[512];
		};	//Exception

		
		class FreeMySQLResult
		{
			public:
				void operator()(MYSQL_RES *res)
				{
					if (res != NULL)
					{
						mysql_free_result(res);
					}
				}
		};


		class ResultSet {
		public:
			explicit ResultSet();
			explicit ResultSet(boost::shared_ptr<MYSQL_RES> result, uint32_t affected_rows, uint64_t lastid);

			~ResultSet();

			bool next();

			MYSQL_FIELD* getFields()  ;

			long long getInt(int index, long long df=0) const;

			long long getInt(const char *name, long long df=0) const;

			std::string getString(int index) const;

			std::string getString(const char *name) const;

			Column get(int index) const;

			Column get(const char *name) const;

			inline uint32_t getColumns() const { return columns_; }

			inline uint32_t getAffectedRows() const { return affected_rows_; }

			inline uint64_t getLastId() const { return lastid_; }

		private:
			boost::shared_ptr<MYSQL_RES> result_;
			MYSQL_ROW row_;
			unsigned long *lengths_ ;

			uint32_t affected_rows_ ;
			uint64_t lastid_ ;
			uint32_t columns_ ;
		};	//ResultSet

		class Statement;
        class MySQLFactory;
		class Connection {
		public:
			friend class Statement;
            friend class MySQLFactory; 

			Connection(const std::string &user, 
                       const std::string &passwd, 
                       const std::string &db, 
                       const std::string &host, 
                       unsigned short port,
                       unsigned int connect_timeout=3,
                       unsigned int read_timeout=30,
                       const std::string& charset="",
                       bool autocommit=true);

			~Connection();

			void setAutoCommit(bool yes);

			void setCharSet(const std::string &name);

			void connect();

			void reconnect();

			Statement createStatement();

			void begin();

			void rollback();

			void commit();

			void disconnect();

			inline bool connected() const { return connected_; }

			bool autocommit() const { return autocommit_ ; }
			
			void release();   //release this connection to the pool
		private:
			bool connected_;
			std::string user_;
			std::string passwd_;
			std::string database_;
			std::string charset_ ;
			std::string host_;
			unsigned short port_;
            unsigned int connect_timeout_;
            unsigned int read_timeout_;
			bool        autocommit_ ;
			MYSQL mysql_;
            MySQLFactory* factory_;
		};	//Connection


		struct Parameter {
			enum ParamType {
				INTEGER		= 0,
				STRING		= 1,
				BLOB		= 2,
				INT_VECTOR	= 3,
			} type;

			union {
				int64_t integer;
				const char *string;
				const std::string *blob;
				const std::vector<int64_t> *int_vec;
			} data;

			explicit Parameter(int8_t i) {
				type = INTEGER;
				data.integer = (int64_t) i;
			}

			explicit Parameter(int16_t i) {
				type = INTEGER;
				data.integer = (int64_t) i;
			}

			explicit Parameter(int32_t i) {
				type = INTEGER;
				data.integer = (int64_t) i;
			}

			explicit Parameter(uint32_t i) {
				type = INTEGER;
				data.integer = (int64_t) i;
			}

			explicit Parameter(int64_t i) {
				type = INTEGER;
				data.integer = i;
			}

			explicit Parameter(const char *str) {
				type = STRING;
				data.string = str;
			}

			explicit Parameter(const std::string &blob) {
				type = BLOB;
				data.blob = &blob;
			}

			explicit Parameter(const std::vector<int64_t> &vec) {
				type = INT_VECTOR;
				data.int_vec = &vec;
			}
		};      //Parameter

		class Statement {
		public:
			explicit Statement(MYSQL *mysql);

			~Statement();

			inline Statement &operator<<(int i) {
				char buf[22];
				int len = snprintf(buf, sizeof(buf), "%d", i);
				sql_.append(buf, len);
				return *this;
			}

			inline Statement &operator<<(uint32_t i) {
				char buf[10];
				int len = snprintf(buf, sizeof(buf), "%u", i);
				sql_.append(buf, len);
				return *this;
			}

			inline Statement &operator<<(uint64_t i) {
				char buf[22];
# if __WORDSIZE == 64
				int len = snprintf(buf, sizeof(buf), "%ld", i);
# else				
				int len = snprintf(buf, sizeof(buf), "%lld", i);
# endif
				sql_.append(buf, len);
				return *this;
			}

			inline Statement &operator<<(const char *str) {
				sql_.append(str, strlen(str));
				return *this;
			}

			inline Statement &operator<<(const std::string &str) {
				sql_.append(str);
				return *this;
			}

			std::string escape(const std::string &fr);

			void prepare(const std::string &sql);

			inline const std::string &preview() const {
				return sql_;
			}

			void bindParams(const std::vector<Parameter> &param);

			ResultSet execute();

		private:
			std::string::size_type bindInt(std::string::size_type pos, const char *from, int64_t i);

			std::string::size_type bindString(std::string::size_type pos, const char *from, const char *data, size_t size);

			MYSQL *mysql_;
			std::string sql_;
		};	//Statment

	}	//mysqldb
}	//server
#endif	//MYSQL_SQL_H
