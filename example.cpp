/***************************************************************************
 *   Copyright (C) 2010-2011 by H. Kappelbauer   *
 *   kappelbauer@plan6.de                   *
 *   All rights reserved.

 *   Redistribution and use in source and binary forms, with or without modification, 
 *   are permitted provided that the following conditions are met:

 *   Redistributions of source code must retain the above copyright notice, this list of 
 *   conditions and the following disclaimer.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *                                                                         *
 ***************************************************************************/
 
 
#include <iostream>
#include <string>

#include <boost/assign/list_of.hpp>

#include "ccredis.h" 

 
using namespace std;
using namespace boost;
using namespace boost::assign;


//-- compiling example.cpp:  g++ -o example -Wall -L. -lhiredis example.cpp
//-- for testing 'example' set $LD_LIBRARY_PATH to the current path

void print( const string &strF )
{
	cout << "\t" << strF << endl;
}


//=====================================================================================+
int main()
//=====================================================================================+
{
	try
	{
		CCRedis _rd; 	//-- creat an instance of CCRedis  -- CCRedis( const std::string strRedisHost = "127.0.0.1", int dwRedisPort = 6379 )
		
		//_rd.auth( "passwd" ); //-- authent. if necessary
		_rd.connect();
		_rd.set( "name:1", "Smith" );	//-- set value
		_rd.set( "name:2", "Miller" );	
		cout << Redis_SPRepl(  _rd.get( "name:1" ) )->getStr() << endl;	//-- output value
		std::string _strName = Redis_SPRepl(  _rd.get( "name:1" ) )->getStr(); //-- assign value
		
	//-- keys ...........................
		Redis_SPRepl _rn( _rd.keys( "name:*" ) );
        Redis_citStr    __iB, __iE;
        cout << "Keys: \n";
        for( tie( __iB, __iE ) = _rn->getArrIt(); __iB != __iE; ++__iB )
            cout << "\t" << *__iB << endl;

	//-- list ...........................
		_rd.rpush( "name_list", "name:0" );
		_rd.rpush( "name_list", "name:1" );
	
	//-- sets ...........................
		_rd.sadd( "fruit", "apple" );
		_rd.sadd( "fruit", "orange" );
		
		Redis_StrVec _vFruits = Redis_SPRepl(  _rd.smembers( "fruit" ) )->getArr();
		cout << "fruits: \n";
		for( Redis_itStr __it = _vFruits.begin(); __it != _vFruits.end(); ++__it )
			cout << "\t" << *__it << endl;
	  //-- or
		Redis_SPRepl _rf( _rd.smembers( "fruit" ) );
		Redis_citStr    __iBF, __iEF;
		tie( __iBF, __iEF ) = _rf->getArrIt();
		cout << "\nor: \n";
		for_each( __iBF, __iEF, print );
	
	//-- hashes .........................
		Redis_mHash _hash;
        _hash["id"] = "0";
        _hash["name"] = "Baur";
        _hash["fname"] = "Jack";
        _rd.hmset( "agent", _hash );
        
        Redis_vHash _tpl = tuple_list_of( "id", lexical_cast<string>( "1" ) )( "name", "Smith" )( "fname", "Bill" );
        _rd.hmset( "victim", _tpl );
        Redis_mHash _hMRes = Redis_SPRepl( _rd.hgetall( "agent" ) )->getMap();
        cout << "\nHash: \nName: " << _hMRes["name"] << " Id: " << _hMRes["id"] << endl;
        int _dwId = lexical_cast<int>( _hMRes["id"] );
        cout << "The id: " << _dwId << endl;
        
    //-- native ......................
    	cout << "\nnative: \nnew index: " << Redis_SPRepl( _rd.native( "incr", "index" ) )->getInt() << endl;
    	Redis_SPRepl( _rd.native( "set", "name:20", "Tester" ) );
     
	
	
	}
	catch( runtime_error( &rtErr ) )
    {
        cerr << "Runtime-Error: " << rtErr.what() <<  endl;
        return 1;
    }
	
	return 0;
}
