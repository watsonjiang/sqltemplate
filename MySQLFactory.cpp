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
                                            config.autocommit),
                                 pool_(0){
             
        }

        void PoolableConnection::close() {
            pool_.releaseConnection(this);
        }

        /* ConnectionPool */
        __ConnectionPoolData::__ConnectionPoolData(unsigned int max_pool_size) {
            pthread_mutex_init(&cache_lock_, NULL);
            pthread_cond_init(&cache_not_empty_cond_, NULL);
            max_pool_size_ = max_pool_size;
        }

        __ConnectionPoolData::~__ConnectionPoolData() {
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

        ConnectionPool::ConnectionPool(unsigned int max_pool_size) {
            pool_data_ = new __ConnectionPoolData(max_pool_size);
            rc_ = new __RefCounter();
            rc_->addRef(); 
            printf("!!!!!construct %p pool %p  rc %d\n", this, pool_data_, rc_->ref_count_);
        }

        ConnectionPool::ConnectionPool(const ConnectionPool& p):pool_data_(p.pool_data_),
                                                                rc_(p.rc_){
            rc_->addRef();
            printf("!!!!!copy construct %p pool %p  rc %d\n", this, pool_data_, rc_->ref_count_);
        }

        ConnectionPool& ConnectionPool::operator =(const ConnectionPool& p) {
            if (&p != this) {
                //TODO: timeout and retry to avoid deadlock
                if(rc_->decRef() == 0) {
                    delete pool_data_;
                    delete rc_;
                }
                pool_data_ = p.pool_data_;
                rc_ = p.rc_;
                rc_->addRef();
            }
            return *this;
        }

        ConnectionPool::~ConnectionPool() {
            printf("!!!!!!destruct %p pool %p rc %d\n", this, pool_data_, rc_->ref_count_);
            if(rc_->decRef() == 0) {
               delete pool_data_;
               delete rc_;
            }
        }

        PoolableConnection* ConnectionPool::getConnection() {
			pthread_mutex_lock(&pool_data_->cache_lock_);
			while(pool_data_->cache_.size()<=0)
			    pthread_cond_wait(&pool_data_->cache_not_empty_cond_, &pool_data_->cache_lock_);
			PoolableConnection *conn = pool_data_->cache_.back();
            conn->pool_ = *this;
			pool_data_->cache_.pop_back();
			pthread_mutex_unlock(&pool_data_->cache_lock_);
            return conn;
        }

        void ConnectionPool::releaseConnection(PoolableConnection *c) {
            pthread_mutex_lock(&pool_data_->cache_lock_);
            pool_data_->cache_.push_back(c);
            c->pool_ = ConnectionPool(0);
            pthread_cond_signal(&pool_data_->cache_not_empty_cond_);
            pthread_mutex_unlock(&pool_data_->cache_lock_);
        }

        __RefCounter::__RefCounter() {
            ref_count_ = 0;
            pthread_mutex_init(&ref_count_lock_, NULL);
        }

        void __RefCounter::addRef() {
            pthread_mutex_lock(&ref_count_lock_);
            ref_count_++;
            pthread_mutex_unlock(&ref_count_lock_);
        }

        int __RefCounter::decRef() {
            unsigned int ret;
            pthread_mutex_lock(&ref_count_lock_);
            ret = --ref_count_; 
            pthread_mutex_unlock(&ref_count_lock_);
            return ret;
        }

		/* MySQLFactory */
		MySQLFactory::MySQLFactory(){
            pthread_mutex_init(&src_map_lock_, NULL); 
        }

		MySQLFactory::~MySQLFactory() {}

        struct __pool_ref_holder {
            __pool_ref_holder():pool_(0){}
            ConnectionPool  pool_;
        };
        static void* __pool_destroyer(void *args) {
            __pool_ref_holder * h = (__pool_ref_holder*)args;
            delete h; //this will trigger the destruction of pool.
        }

		void MySQLFactory::addSource(const std::string &name, const MySQLConfig &config) {
            pthread_mutex_lock(&src_map_lock_);

            ConnectionPool p = ConnectionPool(config.maxconns);
            for(int i=0;i<config.maxconns;i++)
            {
                PoolableConnection *pc = new PoolableConnection(config);
                p.pool_data_->cache_.push_back(pc);
            }
            SRC_MAP::const_iterator it = sources_.find(name);
            if(it == sources_.end()) {
                sources_.insert(std::make_pair(name, p));
            }else{
                //spawn a thread to destruct old pool. to avoid stuck this thread.
                __pool_ref_holder * prh = new __pool_ref_holder;
                prh->pool_ = it->second;
                pthread_t th;
                pthread_create(&th, NULL, __pool_destroyer, (void*)prh);
                sources_.erase(name);
                sources_.insert(std::make_pair(name, p));
            }
            printf("!!!!!!poolname %s pool %p ref %d created.\n", name.data(), p.pool_data_, p.rc_->ref_count_);
            pthread_mutex_unlock(&src_map_lock_);
		}

		Connection *MySQLFactory::getConnection(const std::string &name) {
		    pthread_mutex_lock(&src_map_lock_);
			SRC_MAP::const_iterator it = sources_.find(name);
			pthread_mutex_unlock(&src_map_lock_);
			
			if (it == sources_.end()) {
				return NULL;
			}
            
            ConnectionPool src = it->second;
			PoolableConnection *conn = src.getConnection();

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
