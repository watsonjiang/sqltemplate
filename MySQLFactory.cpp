#include "MySQLFactory.h"
#include <pthread.h>
#include <assert.h>
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
                                 pool_ref_(NULL){
             
        }

        void PoolableConnection::close() {
            pool_ref_->releaseConnection(this);
        }

        /* ConnectionPool */
        ConnectionPool::ConnectionPool(const MySQLConfig& config)
                       :config_(config),
                        ref_count_(0){
            pthread_mutex_init(&cache_lock_, NULL);
            pthread_cond_init(&cache_not_empty_cond_, NULL);
            pthread_mutex_init(&ref_count_lock_, NULL);
            for(int i=0;i<config.maxconns;i++)
            {
                PoolableConnection *pc = new PoolableConnection(config);
                cache_.push_back(pc);
            }
        }

        ConnectionPool::~ConnectionPool() {
            //no ref to the pool == all connections in the pool.
            //disconnect all connections
            assert(cache_.size() == config_.maxconns); 
            for(CACHE_TYPE::iterator it=cache_.begin();
                it!=cache_.end(); ++it) {
                (*it)->disconnect();
                delete (*it);
            }
        }

        void ConnectionPool::addRef() {
            pthread_mutex_lock(&ref_count_lock_);
            ref_count_++;
            pthread_mutex_unlock(&ref_count_lock_);
        }

        int ConnectionPool::decRef() {
            unsigned int ret;
            pthread_mutex_lock(&ref_count_lock_);
            ret = --ref_count_; 
            pthread_mutex_unlock(&ref_count_lock_);
            return ret;
        }

        int ConnectionPool::refCnt() {
            return ref_count_;
        }

        PoolableConnection* ConnectionPool::getConnection() {
            pthread_mutex_lock(&cache_lock_);
            while(cache_.size()<=0)
                pthread_cond_wait(&cache_not_empty_cond_, &cache_lock_);
            PoolableConnection *conn = cache_.back();
            conn->pool_ref_ = ConnectionPoolRef(this);
            cache_.pop_back();
            pthread_mutex_unlock(&cache_lock_);
            return conn;
        }

        void ConnectionPool::releaseConnection(PoolableConnection *c) {
            ConnectionPoolRef ref = c->pool_ref_;
            pthread_mutex_lock(&cache_lock_);
            c->pool_ref_ = ConnectionPoolRef(NULL);
            cache_.push_back(c);
            pthread_cond_signal(&cache_not_empty_cond_);
            pthread_mutex_unlock(&cache_lock_);
        }

        ConnectionPoolRef::ConnectionPoolRef(ConnectionPool * p) {
            pool_ = p;
            if(pool_)
                pool_->addRef();
            //printf("!!!!construct ref %p pool %p rc %d\n", this, p, p==NULL? 0:p->refCnt());
        }

        ConnectionPoolRef::ConnectionPoolRef(const ConnectionPoolRef& p)
                          :pool_(p.pool_){
            if(pool_)
                pool_->addRef();
            //printf("!!!!copy construct ref %p pool %p rc %d\n", this, pool_, pool_==NULL?0:pool_->refCnt());
        }

        ConnectionPoolRef& ConnectionPoolRef::operator =(const ConnectionPoolRef& p) {
            if (&p != this) {
               // printf("!!!!!operator = old ref %p pool %p rc %d\n", this, pool_, pool_==NULL?0:pool_->refCnt());
                if(pool_!=NULL && pool_->decRef() == 0) {
                    delete pool_;
                }
                pool_ = p.pool_;
                if(pool_)
                    pool_->addRef();
                //printf("!!!!!operator = new ref %p pool %p rc %d\n", this, pool_, pool_==NULL?0:pool_->refCnt());
            }
            return *this;
        }
    
        ConnectionPool* ConnectionPoolRef::operator ->() {
            return pool_; 
        }

        ConnectionPoolRef::~ConnectionPoolRef() {
            //printf("!!!!!!before destruct ref %p pool %p rc %d\n", this, pool_, pool_==NULL?0:pool_->refCnt());
            if(pool_!=NULL && pool_->decRef() == 0) {
               delete pool_;
            }
        }

        /* MySQLFactory */
        MySQLFactory::MySQLFactory(){
            pthread_mutex_init(&src_map_lock_, NULL); 
        }

        MySQLFactory::~MySQLFactory() {}

        void MySQLFactory::addSource(const std::string &name, const MySQLConfig &config) {
            pthread_mutex_lock(&src_map_lock_);
            printf("!!!!add source %s %s\n", name.data(), config.host.data());
            ConnectionPoolRef pr(new ConnectionPool(config));
            SRC_MAP::const_iterator it = sources_.find(name);
            if(it == sources_.end()) {
                sources_.insert(std::make_pair(name, pr));
            }else{
                sources_.erase(name);
                sources_.insert(std::make_pair(name, pr));
            }
            pthread_mutex_unlock(&src_map_lock_);
            //printf("!!!!!addSource %s \n", name.data());
        }

        Connection *MySQLFactory::getConnection(const std::string &name) {
            pthread_mutex_lock(&src_map_lock_);
            SRC_MAP::const_iterator it = sources_.find(name);
            pthread_mutex_unlock(&src_map_lock_);
            
            if (it == sources_.end()) {
                return NULL;
            }
            
            ConnectionPoolRef src = it->second;
            PoolableConnection *conn = src->getConnection();

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
        
    }    //mysqldb
}    //server
