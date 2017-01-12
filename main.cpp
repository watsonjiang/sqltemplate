#include "MySQLTemplate.h"
#include <string>
#include <pthread.h>
#include <time.h>

using namespace std;
using namespace server::mysqldb;

void
sample(MySQLTemplate& template_)
{
    SingleResultSet cb;
	const char * sql = "select count(*) from emp";
    bool sqlRet = template_.execute(&cb, sql);
	if(!sqlRet) {
	     printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
		 return;
	} 
	printf("employ total: %s\n", cb.result_[0].data());
}

void
sample1(MySQLTemplate& template_)
{
    const char *sql = "update emp set name=:1 where id=:2";

	std::vector<Parameter> params;
	string new_name = "watson";
	int id = 2;
	NOPCallback cb;
	
	bool sqlRet = template_.execute(&cb, sql, new_name, id);
    if (!sqlRet) {
        printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
    }
    printf("update done!\n");
}

struct Emp {
    int id;
	string name;
};

void
sample2(MySQLTemplate& template_)
{
	struct MyCustomCallBack : public NOPCallback
	{
		void onResult(ResultSet &result)
		{
			while (result.next())
			{
				Emp* emp = new(Emp);
				emp->id = result.getInt(1);
				emp->name = result.getString(2);
				emp_list.push_back(emp);
			}
		}
		vector<Emp*> emp_list ;
	} cb;
	
	const char *sql = "select id, name from emp";
	bool sqlRet = template_.execute(&cb, sql);
	if(!sqlRet) {
	    printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
		return;
	}
   	printf("employ total: %d\n", cb.emp_list.size());
    for(vector<Emp*>::iterator it = cb.emp_list.begin();
        it!=cb.emp_list.end();
        ++it) {
     	printf("%d\t%s\n", (*it)->id, (*it)->name.data());
    }
}

void
sample3(MySQLTemplate& template_)
{
    const char *sql = "select sleep(10)";

	NOPCallback cb;
	
	bool sqlRet = template_.execute(&cb, sql);
    if (!sqlRet) {
        printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
    }
    printf("sleep done!\n");
}


void*
thread_loop(void* p)
{
    MySQLTemplate * template_ = (MySQLTemplate *)p;
    for(;;) {
        try{
        //sample(template_);
        //sample1(template_);
        //sample2(*template_);
        sample3(*template_);
        }catch(Exception &e) {
            printf("error: %s\n", e.what()); 
        }catch (...) {
            printf("Unknown error!\n");
        }
    }
}


int
main()
{
    MySQLConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 3306;
    cfg.user = "watson";
    cfg.passwd = "wapwap12";
    cfg.database = "testdb";
    cfg.maxconns = 1;
    cfg.autocommit = 1;
    server::mysqldb::MYSQL_FACTORY::instance().addSource("testdb", cfg);
    MySQLTemplate template_("testdb");   
    pthread_t th;
    for(int i=0;i<3;i++) {
        pthread_create(&th, NULL, thread_loop, (void*)&template_);
    }

    for(;;) {
        sleep(1);
    }
    return 0;
}
