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
        class ConnectionPool;
        typedef boost::shared_ptr<ConnectionPool> POOL_TYPE;
        typedef std::map<std::string, POOL_TYPE> SRC_MAP;

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

        class ConnectionPool;
        class PoolableConnection : public Connection {
        public:
            PoolableConnection(const MySQLConfig& config);
            void close(); 

            POOL_TYPE  pool_;
        };

	    class ConnectionPool {
        friend class PoolableConnection;
        friend class MySQLFactory;
        public:
            ConnectionPool(unsigned max_pool_size);
            ~ConnectionPool();
        protected:
            void releaseConnection(PoolableConnection * c);
        private:
            std::vector<PoolableConnection*> cache_;   //pooling connections
			pthread_mutex_t cache_lock_;
			pthread_cond_t cache_not_empty_cond_;
			unsigned int max_pool_size_;
		};


		class MySQLFactory {
		public:
			MySQLFactory();

			virtual ~MySQLFactory();

			void addSource(const std::string &name, const MySQLConfig &config);

			Connection *getConnection(const std::string &name);  //allocate a connection from pool

		private:
			SRC_MAP sources_;
			pthread_mutex_t src_map_lock_;
		};
		typedef singleton_default<MySQLFactory> MYSQL_FACTORY ;

	}
}


#endif
