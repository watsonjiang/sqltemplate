#ifndef MYSQL_FACTORY_H
#define MYSQL_FACTORY_H

#include "MySQLDriver.h"
#include "singleton.h"
#include <pthread.h>
#include <map>
#include <vector>
#include <boost/shared_ptr.hpp>

namespace server {
	namespace mysqldb {

	    class Connection;
		struct MySQLConfig {
            MySQLConfig():autocommit(1), read_timeout(30), connect_timeout(3){}
			std::string host;
			unsigned short port;
			std::string user;
			std::string passwd;
			std::string database;
			std::string charset;
            unsigned int read_timeout;
            unsigned int connect_timeout;
			bool		autocommit;
            unsigned int maxconns;
		};

		class MySQLFactory {
		public:
			MySQLFactory();

			virtual ~MySQLFactory();

			void addSource(const std::string &name, const MySQLConfig &config);

			Connection *getConnection(const std::string &name);  //allocate a connection from pool

            void releaseConnection(std::string& name, Connection * c);

		private:
			struct Source {
                std::vector<Connection*> cache;   //pooling connections
				pthread_mutex_t cache_lock;
				pthread_cond_t cache_not_empty_cond;
				MySQLConfig config;
			};
            typedef std::map<std::string, boost::shared_ptr<Source> > SRC_MAP;
			SRC_MAP sources_;
			pthread_mutex_t src_map_lock;
		};
		typedef singleton_default<MySQLFactory> MYSQL_FACTORY ;

	}
}


#endif
