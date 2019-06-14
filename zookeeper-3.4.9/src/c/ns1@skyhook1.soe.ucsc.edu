#include <iostream>
#include <bits/stdc++.h>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <queue>
#include <unistd.h>

using namespace std;

enum Type{
	CREATE = 1,
	SET,
	GET,
	DELETE
};

struct request{
	int IOType; // IOTYPE
	string table;
	char data;
	int isolation; // 0-UC, 1-C
	int tid;
	int timestamp;
};

struct Tcommit {
	char TableName;
	int DoneTime;
	char valueToWrite;

};

//Todo: Add multithreading for processing requests from queues

string T="";
void execZookeeperClient( Type t , string HOST, string name, string data, bool& result );

void queueRequests( vector<request> requests2, queue<request>& pr )
{
	for( int i=0; i< requests2.size(); ++i)
	{
		pr.push(requests2[i]);
		//cout<<requests2[i].data<<"--\t";
		
	}
}

// Need to find out how to parse get API to get table output
bool isTableBusy(request rt)
{
	string tableName = rt.table;

	tableName = "/Table" + tableName + ".busy"; 

	bool result;
	//execZookeeperClient( GET, "127.0.0.1:2181", tableName, "", result);

	execZookeeperClient(GET, "127.0.0.1:2181", "/TableT.busy", "", result);

	return result;
}

// Logic which maps Table name to a particular number which will be index for waitQueue array
int mapTabletoNumber( request rt )
{

	int i=0;
	return i;


}

void initWaitQueue( vector<queue<request>>& waitQueue )
{
	for( int i=0; i< 5; ++i)
		waitQueue.push_back(queue<request>());
}

// This function will decide whether to add the request to waiting queue or to send to Ceph for actual write/read 
void processRequests( queue<request>& pr, vector<queue<request>> waitQueue, queue<request>& canProcess)
{
	for( int j=0; j< 2 ; ++j)
	{
		if( pr.empty())
			return;

		request rt = pr.front();
		bool isBusy = isTableBusy( rt  );

		int i = mapTabletoNumber(rt);
		if( isBusy && rt.isolation==1 )
		{
			waitQueue[i].push(rt);

		}
		else
		{	
			canProcess.push(rt);
			bool result;
			if( rt.IOType == 1)
				execZookeeperClient(SET, "127.0.0.1:2181", "/TableT.busy", "T", result );
			sleep(5);
		}

		pr.pop();
	}

}

void processWaitQueue( vector<queue<request>> waitQueue, queue<request>& canProcess)
{

        //int i = mapTabletoNumber();
	for( int i=0; i<1; ++i)
	{	
		if( !waitQueue[i].empty())
		{
			request rt = waitQueue[i].front();
			if( !isTableBusy( rt ))
			{
				waitQueue[i].pop();
				canProcess.push(rt);
			}
		}
	}

}

void execZookeeperClient( Type t , string HOST, string name, string data, bool& result )
{

	string cmd = " cmd:";

	switch(t)
	{
		case CREATE:
			cmd = cmd + "\"create ";
			cmd = cmd + name + "\" ";
			break;
		case SET:
			cmd = cmd + "\"set ";
			cmd+= name;
			cmd = cmd + " " + data + "\"";
			if( data == "T")
		        	cout<<"Marking Table T as busy...\n";
			else	
		        	cout<<"Marking Table T as not busy...\n";
			break;
		case GET:
			cmd = cmd + "\"get ";
			cmd+= name + "\"";
			cout<<"Fetching value of Table T status from Znode "<<name<< "...\n";
			break;
		case DELETE:
			cmd = cmd + "delete";
			cmd+= name;
			break;
	}

	string cliCmd;
	if( t == GET )
	 	cliCmd = "./cli " + HOST + cmd + " 2>&1 | awk -F \"Znode =\" \'{print $2}\'";
	else
		cliCmd = "./cli " + HOST + cmd + " 2>&1";

	//system( cliCmd.c_str() );
	//Now print the parsed output
	FILE* fp=popen(cliCmd.c_str(), "r");

	char buff[100];
	while(fgets(buff, 100, fp) != NULL)
	{
	}

	if( t == GET) {
		/*char buff[100];
		while(fgets(buff, 100, fp) != NULL)
		{
		}*/

		printf("Value stored in znode is %s", buff);
		if( buff[1] == 'F') {
			result = false;
		}
		else if( buff[1] == 'T' ) {
			result = true;
		}
	}
	pclose(fp);
}

int main()
{
	//priority_queue<request3> prQueue;
	queue<request> requestQueue;  // Change this to priority queue

	//priority_queue<request3> prWaiting;
	vector<queue<request>> waitQueue;
	//string requests[7]={"write T abcdef", "read T 1", "write Q efghrt", "red Q 1"};

	queue<request> canProcess;
	vector<request> requests;

	vector<int> doneRequest;
	int tid=500;
	for(int i=0; i < 3; ++i)
	{
		request st;
		if(i ==0)
			st.IOType=1;
		else
			st.IOType = 2;

		st.table = "T";
		if( i == 1)
			st.isolation=0;
		else if( i == 2)
			st.isolation=1;
		else
			st.isolation=1; // 1 for read committed

		st.tid = tid++;

		char nn = 'a' + i; 
		st.data=nn;
		requests.push_back(st);

	}

	// Add priority queue later
	requests[0].timestamp=1;
	requests[1].timestamp=3;
	requests[2].timestamp=2;
	bool result;
	initWaitQueue(waitQueue);
	// Add requests vector to priority queue
	queueRequests(requests, requestQueue);

	//execZookeeperClient(CREATE, "127.0.0.1:2181", "/TableT.busy", "", result);
	//
	
	//execZookeeperClient(CREATE, "127.0.0.1:2181", "/TableQ.obj", "", result);
	 
	//
	// This is the sleep cycle that we have discussed here
	for( int i=0; i<2 ; ++i)
	{

		//Mark done queries now

		processRequests(requestQueue, waitQueue, canProcess );
		while( !canProcess.empty())
		{
			request r=canProcess.front();
        		canProcess.pop();

			sleep(5);
       			// Markdone 
			if(r.IOType == 1)
			{	bool result;
				cout<<"Processing Transaction with Tid:"<<r.tid<<'\n';
				cout<<"Writing data to the Table T...\n";
				T+=r.data;
				doneRequest.push_back(r.tid);	
				execZookeeperClient(SET, "127.0.0.1:2181", "/TableT.busy", "F", result);
				sleep(3);

			}
			else 
			{
				cout<<"Processing Transaction with Tid:"<<r.tid<<'\n';
				if(r.isolation == 1)
					cout<<"Tid:"<<r.tid<<" will see state after tid:"<<doneRequest[doneRequest.size() - 1]<<"\n";
				else
					cout<<"Tid:"<<r.tid<<" will see either state after tid:"<<doneRequest[doneRequest.size() - 1]<<" or state after uncommitted write transactions which are ready to be processed"<<"\n";

				cout<<"Contents of Table T is: "<<T<<'\n';
			}
			processWaitQueue(waitQueue, canProcess);
		}
	}

	//processRequests(requestQueue, waitQueue, canProcess );*/


	// ---- This code is to call znode APIs required to be added------------
	//execZookeeperClient(SET, "127.0.0.1:2181", "/TableT.busy", "F", result);
	//sleep(10);
	//execZookeeperClient(GET, "127.0.0.1:2181", "/TableT.busy", "", result);
		

	result = isTableBusy(requests[0]);
	if(result)
		cout<<"\nTable T is busy actually.";
	else
		cout<<"\nTable T is free, process this transaction.";
		//findTable which will 
}

