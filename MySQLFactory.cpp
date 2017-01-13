#include "MySQLFactory.h"
#include <pthread.h>

namespace server {
	namespace mysqldb {
        /* PoolableConnection */
        PoolableConnection::PoolableConnection(const MySQLConfig& config):
                                 Connection(config.user, 
                                            config.passwd, 
                                            config.database,
                                            config.host,
                                            config.port,
                                            config.connect_timeout,
                                            config.read_timeout,
                                            config.charset,
                                            config.autocommit) {
             
        }

        void PoolableConnection::close() {
            pool_->releaseConnection(this);
            pool_.reset();
        }

        /* ConnectionPool */
        ConnectionPool::ConnectionPool(unsigned int max_pool_size):max_pool_size_(max_pool_size) {
            pthread_mutex_init(&cache_lock_, NULL);
            pthread_cond_init(&cache_not_empty_cond_, NULL);
        }

        void ConnectionPool::releaseConnection(PoolableConnection *c) {
            pthread_mutex_lock(&cache_lock_);
            cache_.push_back(c);
            pthread_cond_signal(&cache_not_empty_cond_);
            pthread_mutex_unlock(&cache_lock_);
        }

        ConnectionPool::~ConnectionPool() {
            //disconnect all under connections
            int n = max_pool_size_;
            while(0 < n) {
                pthread_mutex_lock(&cache_lock_);
     	 	    while(cache_.size()<=0) {
			        pthread_cond_wait(&cache_not_empty_cond_, &cache_lock_);
                }
                PoolableConnection* c = cache_.back();
                cache_.pop_back();
                pthread_mutex_unlock(&cache_lock_);
                c->disconnect(); 
                delete c;
                n--;
            }
        }

		/* MySQLFactory */
		MySQLFactory::MySQLFactory(){
            pthread_mutex_init(&src_map_lock_, NULL); 
        }

		MySQLFactory::~MySQLFactory() {}

        struct __pool_ref_holder {
           POOL_TYPE  pool_;
        };
        static void* __pool_destroyer(void *args) {
            __pool_ref_holder * h = (__pool_ref_holder*)args;
            delete h; //this will trigger the destruction of pool.
        }

		void MySQLFactory::addSource(const std::string &name, const MySQLConfig &config) {
            pthread_mutex_lock(&src_map_lock_);

            ConnectionPool *p = new ConnectionPool(config.maxconns);
            SRC_MAP::mapped_type v = SRC_MAP::mapped_type(p);
            for(int i=0;i<config.maxconns;i++)
            {
                PoolableConnection *pc = new PoolableConnection(config);
                pc->pool_ = v;
                v->cache_.push_back(pc);
            }
            SRC_MAP::const_iterator it = sources_.find(name);
            if(it == sources_.end()) {
                sources_.insert(std::make_pair(name, v));
            }else{
                //spawn a thread to destruct old pool. to avoid stuck this thread.
                __pool_ref_holder * prh = new __pool_ref_holder;
                prh->pool_ = it->second;
                pthread_t th;
                pthread_create(&th, NULL, __pool_destroyer, (void*)prh);
                sources_.erase(name);
                sources_.insert(std::make_pair(name, v));
            }
            pthread_mutex_unlock(&src_map_lock_);
		}

		Connection *MySQLFactory::getConnection(const std::string &name) {
		    pthread_mutex_lock(&src_map_lock_);
			SRC_MAP::const_iterator it = sources_.find(name);
			pthread_mutex_unlock(&src_map_lock_);
			
			if (it == sources_.end()) {
				return NULL;
			}
            
            POOL_TYPE src = it->second;
			pthread_mutex_lock(&src->cache_lock_);
			while(src->cache_.size()<=0)
			    pthread_cond_wait(&src->cache_not_empty_cond_, &src->cache_lock_);
			PoolableConnection *conn = src->cache_.back();
            conn->pool_ = src;
			src->cache_.pop_back();
			pthread_mutex_unlock(&src->cache_lock_);

			if (!conn->connected()) {
				//YY_LOG_ERROR( "mysql db:%s not connect, try reconnect", name.c_str());

				try {
					conn->connect();
				} catch (Exception &e) {
					/*YY_LOG_ERROR( "connect mysql://%s:***@%s:%d/%s failed: %s",
						src->config.user.c_str(),
						src->config.host.c_str(),
						src->config.port,
						src->config.database.c_str(),
						e.what());*/
					conn->disconnect();
				}

				//conn->setCharSet("utf8");
			}
			return conn;
		}
        
	}	//mysqldb
}	//server
