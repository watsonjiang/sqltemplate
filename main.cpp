#include "MySQLTemplate.h"
#include <string>
#include <pthread.h>
#include <time.h>
#include <mysql.h>
#include <stdlib.h>

using namespace std;
using namespace server::mysqldb;

void
sample(MySQLTemplate& template_)
{
    SingleResultSet cb;
    const char * sql = "select count(*) from emp";
    if(template_.execute(&cb, sql)) {
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
    
    if(template_.execute(&cb, sql, new_name, id)) {
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
    if(template_.execute(&cb, sql)) {
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
    
    if(template_.execute(&cb, sql)) {
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
        int idx = rand() % 4;
        switch(idx) {
        case 0:
        sample(*template_);break;
        case 1:
        sample1(*template_);break;
        case 2:
        sample2(*template_);break;
        case 3:
        sample3(*template_);break;
        }
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
    mysql_library_init(0, NULL, NULL);
    MySQLConfig cfg;
    cfg.host = "localhost";
    cfg.port = 3306;
    cfg.user = "watson";
    cfg.passwd = "wapwap12";
    cfg.database = "testdb";
    cfg.read_timeout = 3;
    cfg.maxconns = 10;
    cfg.autocommit = 1;
    server::mysqldb::MYSQL_FACTORY::instance().addSource("testdb", cfg);
    MySQLTemplate template_("testdb");   
    pthread_t th;
    for(int i=0;i<30;i++) {
        pthread_create(&th, NULL, thread_loop, (void*)&template_);
    }
   
    char* hosts[] = {"localhost", "133.1.2.3", "134.32.3.1"};
    int n = 0;
    for(;;) {
        sleep(3);
        cfg.host = string(hosts[n]);
        printf("change source to %s\n", hosts[n]);
        server::mysqldb::MYSQL_FACTORY::instance().addSource("testdb", cfg);
        n = n + 1;
        n = n % 3;
        printf("n %d\n", n);
    }
    
    return 0;
}
