#include "MySQLFactory.h"

namespace server {
	namespace mysqldb {

		/* MySQLFactory */
		MySQLFactory::MySQLFactory(){
            pthread_mutex_init(&src_map_lock, NULL); 
        }

		MySQLFactory::~MySQLFactory() {}

		void MySQLFactory::addSource(const std::string &name, const MySQLConfig &config) {
			std::pair<std::string, Source *> p;
			p.first = name;
			p.second = new Source;
			p.second->config = config;
            pthread_mutex_init(&p.second->cache_lock, NULL);
            pthread_cond_init(&p.second->cache_not_empty_cond, NULL);
            for(int i=0;i<config.maxconns;i++)
            {
                p.second->cache.push_back(new Connection());
            }
			sources_.insert(p);
		}

		Connection *MySQLFactory::getConnection(const std::string &name) {
		    pthread_mutex_lock(&src_map_lock);
			SRC_MAP::const_iterator it = sources_.find(name);
			pthread_mutex_unlock(&src_map_lock);
			
			if (it == sources_.end()) {
				return NULL;
			}
            
            Source * src = it->second.get();
			pthread_mutex_lock(&src->cache_lock);
			while(src->cache.size()<=0)
			    pthread_cond_wait(&src->cache_not_empty_cond, &src->cache_lock);
			Connection *conn = src->cache.back();
			src->cache.pop_back();
			pthread_mutex_unlock(&src->cache_lock);

			if (!conn->connected()) {
				//YY_LOG_ERROR( "mysql db:%s not connect, try reconnect", name.c_str());

				try {
					conn->connect(
						src->config.user,
						src->config.passwd,
						src->config.database,
						src->config.host,
						src->config.port,
						src->config.charset,
						src->config.autocommit);
				} catch (Exception &e) {
					/*YY_LOG_ERROR( "connect mysql://%s:***@%s:%d/%s failed: %s",
						src->config.user.c_str(),
						src->config.host.c_str(),
						src->config.port,
						src->config.database.c_str(),
						e.what());*/
					conn->disconnect();
					return NULL;
				}

				//conn->setCharSet("utf8");
			}

			return conn;
		}
        
        void MySQLFactory::releaseConnection(std::string& name, Connection *c) {
            pthread_mutex_lock(&src_map_lock);
            SRC_MAP::const_iterator it = sources_.find(name);
            pthread_mutex_unlock(&src_map_lock);
            
            if (it == sources_.end()) {
                //shoud not reach here, log and panic
                exit(1);
            }
            Source * src = it->second.get();
            pthread_mutex_lock(&src->cache_lock);
            src->cache.push_back(c);
            pthread_cond_signal(&src->cache_not_empty_cond);
            pthread_mutex_unlock(&src->cache_lock);
            return;
        }
	}	//mysqldb
}	//server
