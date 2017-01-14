#ifndef MYSQL_FACTORY_H
#define MYSQL_FACTORY_H

#include "MySQLDriver.h"
#include "singleton.h"
#include <pthread.h>
#include <map>
#include <vector>

namespace server {
    namespace mysqldb {
        class ConnectionPool;

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
            bool        autocommit;
            unsigned int maxconns;
        };

        class PoolableConnection;
        
        class ConnectionPool {
        public:
            ConnectionPool(const MySQLConfig& config);
            ~ConnectionPool();
             
            PoolableConnection *getConnection();
            void releaseConnection(PoolableConnection * c);
         
            void addRef();
            int decRef();
            int refCnt();

        private:
            typedef std::vector<PoolableConnection*>   CACHE_TYPE;
            CACHE_TYPE cache_;   //pooling connections
            pthread_mutex_t cache_lock_;
            pthread_cond_t cache_not_empty_cond_;
            MySQLConfig config_;

            int ref_count_;
            pthread_mutex_t ref_count_lock_;
        };


        class ConnectionPoolRef {
        public:
            ConnectionPoolRef(ConnectionPool*);
            ConnectionPoolRef(const ConnectionPoolRef&);
            ConnectionPoolRef& operator =(const ConnectionPoolRef&);
            ConnectionPool* operator->();
            ~ConnectionPoolRef();

        private:
            ConnectionPool *pool_;
        };

        class PoolableConnection : public Connection {
        public:
            PoolableConnection(const MySQLConfig& config);
            void close(); 

            ConnectionPoolRef pool_ref_;
        };


        class MySQLFactory {
        public:
            MySQLFactory();

            virtual ~MySQLFactory();

            void addSource(const std::string &name, const MySQLConfig &config);

            Connection *getConnection(const std::string &name);  //allocate a connection from pool

        private:
            typedef std::map<std::string, ConnectionPoolRef> SRC_MAP;
            SRC_MAP sources_;
            pthread_mutex_t src_map_lock_;
        };
        typedef singleton_default<MySQLFactory> MYSQL_FACTORY ;

    }
}


#endif
