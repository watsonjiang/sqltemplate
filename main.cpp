#include "MySQLTemplate.h"
#include <string>


using namespace std;
using namespace server::mysqldb;

void
sample(MySQLTemplate& template_)
{
    SingleResultSet cb;
	const char * sql = "select count(*) from employee";
    bool sqlRet = template_.execute(&cb, sql);
	if(sqlRet) {
	     printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
		 return;
	} 
	printf("employ total: %s", cb.result_[0].data());
}

void
sample1(MySQLTemplate& template_)
{
    const char *sql = "update employee set name=:1 where id=:2";

	std::vector<Parameter> params;
	string new_name = "watson";
	int id = 1;
	NOPCallback cb;
	
	bool sqlRet = template_.execute(&cb, sql, new_name, id);
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
	
	const char *sql = "select id, name from employee";
	bool sqlRet = template_.execute(&cb, sql);
	if(sqlRet) {
	    printf("Fail to execute sql: %s errcode: %d errmsg:%s\n",
                sql, cb.error_, cb.errorMsg_.data());
		return;
	}
	printf("employ total: %s", cb.emp_list.size());
}

int
main()
{
   MySQLConfig cfg;
   cfg.host = "127.0.0.1";
   server::mysqldb::MYSQL_FACTORY::instance().addSource("testdb", cfg);
   MySQLTemplate template_("testdb");   
   sample(template_);
   sample1(template_);
   sample2(template_);
   return 0;
}