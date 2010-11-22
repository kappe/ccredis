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


#ifndef CREDIS_H
#define CREDIS_H

#include <cctype>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <stdexcept>

#include <boost/format.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/variant.hpp>

#include "anet.h"
#include "hiredis.h"


//-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+
class CCRedis 
//-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+
{
public:
	enum 											eREPLYTYPE { ERROR, STRING, ARRAY, INTEGER, NIL };
	typedef std::vector<std::string>				T_vString;
	typedef std::map<std::string, std::string>		T_mMap;
	typedef T_vString::const_iterator				T_citString;
	typedef boost::tuple<std::string, std::string>	T_tupRedis;
	typedef std::vector<T_tupRedis>					T_vTupRedis;
	typedef boost::tuple<T_citString, T_citString>	T_tupItRedis;
	

	//-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+
	class CCRedisReply 
	//-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+
	{
		
	protected:
		eREPLYTYPE		m_eReplyType;
		redisReply		*m_Reply;
		std::string		m_strReply;
		long long		m_dw64Reply;
		T_vString		m_vReply;
		T_mMap			m_mMap;
		T_vTupRedis		m_vTup;
		
	protected:	
	//-- mapping redisReply to internal data structure
		void	map( redisReply* RR ) {
			m_Reply = RR;

			m_eReplyType = static_cast<eREPLYTYPE>( m_Reply->type );
			if( static_cast<eREPLYTYPE>( m_Reply->type ) == STRING )
				m_strReply.assign( m_Reply->reply );
			else if( static_cast<eREPLYTYPE>( m_Reply->type ) == INTEGER )
				m_dw64Reply = m_Reply->integer;
			else if( static_cast<eREPLYTYPE>( m_Reply->type ) == ARRAY )
			{
				for( register unsigned long __l = 0; __l < m_Reply->elements; ++__l )
					m_vReply.push_back( m_Reply->element[__l]->reply );	
			}
			else if( static_cast<eREPLYTYPE>( m_Reply->type ) == ERROR )
				throw( std::runtime_error( std::string( "CCRedisReply-Exception: " ) + std::string( m_Reply->reply ? m_Reply->reply : "" ) ) );
			else if( static_cast<eREPLYTYPE>( m_Reply->type ) == NIL )
				m_strReply.erase(); //assign( ":NIL:" );
		};
				
				
	public:
		const eREPLYTYPE  	replyType() const { return m_eReplyType; }
		
		const long long		getInt() const {
			if( m_eReplyType != INTEGER )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type (not an INTEGER)" ) );
			return m_dw64Reply;
		};
		const std::string	getStr() const {
			if( m_eReplyType != STRING && m_eReplyType != NIL )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type (not a STRING)" ) );
			return m_strReply;
		};	
		const T_vString& getArr() const {
			if( m_eReplyType != ARRAY && m_eReplyType != NIL )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type (not an ARRAY)" ) );
			return m_vReply;
		};
		const T_tupItRedis getArrIt() {
			if( m_eReplyType != ARRAY && m_eReplyType != NIL )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type" ) );
			return( boost::make_tuple( m_vReply.begin(), m_vReply.end() ) );
		};	
		const T_mMap& getMap() {
			if( m_eReplyType != ARRAY && m_eReplyType != NIL )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type" ) );
			register int __cnt( m_vReply.size() );
			for( ;__cnt > 0; __cnt -= 2 ) {
				m_mMap[m_vReply[__cnt-2]] = m_vReply[__cnt-1];
			}
			return m_mMap;	
		};
		const T_vTupRedis& getHash() {
			if( m_eReplyType != ARRAY && m_eReplyType != NIL )
				throw( std::runtime_error( "CCRedisReply-Exception: wrong type" ) );
			register int __cnt( m_vReply.size() );
			for( ;__cnt > 0; __cnt -= 2 ) { 
				T_tupRedis __t( boost::make_tuple( m_vReply[__cnt-2], m_vReply[__cnt-1] ) );
				m_vTup.push_back( __t );
			}
			return m_vTup;	
		};
		
		const redisReply* assign( redisReply* RR ) {
			map( RR );
			return m_Reply;
		};
		CCRedisReply( redisReply *rr = NULL) : m_Reply( rr ) {
			if( rr )
				map( rr );
		};
		virtual ~CCRedisReply() {
			if( m_Reply )
			{
				freeReplyObject( m_Reply );
				//cout << "DELETE" << endl;
				m_Reply = NULL;
			}
		};
	};	
  //-- ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::+ 
private:
	template <typename T> struct F_Serialize {
		std::ostream 	&m_os;
		
		void operator () ( const T &t /*const std::string &strKey*/ ) {
			m_os << " " << t;
		};
		F_Serialize( std::ostream &os ) : m_os( os ) {};
	};
	
	struct F_SerializeMap {
		std::ostream 	&m_os;
		
		void operator () ( const std::pair<std::string, std::string> &p ) {
			m_os << "\r\n$" << p.first.length() << "\r\n" << p.first << "\r\n$" << p.second.length() << "\r\n" << p.second;
		};
		F_SerializeMap( std::ostream &os ) : m_os( os ) {};
	};
	struct F_SerializeTup {
		std::ostream 	&m_os;
		
		void operator () ( const T_tupRedis &tu ) {
			m_os << "\r\n$" << boost::get<0>( tu ).length() << "\r\n" << boost::get<0>( tu ) << "\r\n$" << boost::get<1>( tu ).length() << "\r\n" << boost::get<1>( tu );
		};
		F_SerializeTup( std::ostream &os ) : m_os( os ) {};
	};
	

protected:
	typedef int									RH;
	typedef boost::scoped_ptr<CCRedisReply>		SP_RedRpl;
			
protected:
	RH				m_rh;
	std::string		m_strRedisHost;
	int				m_dwRedisPort;
	

private:
	int protReply() {
		int 		__dwRet( 0 );
		eREPLYTYPE	__type;
		std::string __strNumBuff;
		for( int __i = 0;; ++__i )
		{
			char __c;
			
			if( read( m_rh, &__c, 1 ) == -1 )
				throw( std::runtime_error( "Redis-Protocol-Exception: channel" ) );
//cout << __c << "(" << __i << ")";
			if( __i == 0  ) {
				switch( __c ) {
				case '+':	__type = STRING; __dwRet = 1; break;
				case ':':	__type = INTEGER; break;
				case '-':	__type = ERROR; break;
				case '$':	break;
				case '*': 	break;
				default: throw( std::runtime_error( "Redis-Protocol-Exception: invalid response" ) );
				}
			}
			else if( __c == '\r' )
				continue;
			else if( __c == '\n' )
				break;
			else  {
				__strNumBuff.append( 1, __c );	
			}
		}
//cout << endl;
		if( __type == STRING )
			return __dwRet;
		else if( __type == INTEGER )
			__dwRet = atoi( __strNumBuff.c_str() );
		else if( __type == ERROR ) {
			std::string __strMsg = std::string( "Redis-Protocol-Exception: " ) + __strNumBuff;
			throw( std::runtime_error( __strMsg ) );	
		}
		return __dwRet;
	};
	

public:
	CCRedisReply* get( std::string strKey ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey;
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str()  ) );
	};
	CCRedisReply* keys( std::string strKey ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey;
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str()  ) );
	};
	CCRedisReply* mget( const T_vString &vKeys ) {
		std::ostringstream __osCmd;
		__osCmd << __FUNCTION__;
		for_each( vKeys.begin(), vKeys.end(), F_Serialize<std::string>( __osCmd ) );
		return new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str()  ) );
	};  
	
  //--
	bool set( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) ) )->replyType() != ERROR ? true : false );
	};
	int setnx( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) ) )->getInt() );
	};
	CCRedisReply* getset( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) );
	};
  //--  
  	int mset( const T_mMap &m, const std::string strCmd = "MSET" ) {
		std::ostringstream 	__osCmd;
		__osCmd << "*" << m.size()*2 + 1 << "\r\n" << "$" << strCmd.length() << "\r\n" << strCmd;
		for_each( m.begin(), m.end(), F_SerializeMap( __osCmd ) );
		__osCmd << "\r\n";
		anetWrite( m_rh, const_cast<char*>( __osCmd.str().c_str() ), __osCmd.str().length() );
		return( protReply() );
		//return( SP_RedRpl( new CCRedisReply( redisGetReply( m_rh ) ) )->replyType() != ERROR ? true : false );
	};
	int mset( const T_vTupRedis &vTup, const std::string strCmd = "MSET" ) {
		std::ostringstream 	__osCmd;
		__osCmd << "*" << vTup.size()*2 + 1 << "\r\n" << "$" << strCmd.length() << "\r\n" << strCmd;
		for_each( vTup.begin(), vTup.end(), F_SerializeTup( __osCmd ) );
		__osCmd << "\r\n";
		anetWrite( m_rh, const_cast<char*>( __osCmd.str().c_str() ), __osCmd.str().length() );
		return( protReply() );
	};
  //-- SETS
  	int sadd( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) ) )->getInt() );
	};
  	CCRedisReply* smembers( std::string strKey ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey;
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str() ) );
	};  
	int	scard( std::string strKey ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey;
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, strKey.c_str() ) ) )->getInt() );
	};
  //-- LIST
  	int rpush( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) ) )->getInt() );	
	};	
	int lpush( std::string strKey, std::string strVal ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " %b" );
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str(), strVal.c_str(), strVal.length()  ) ) )->getInt() );	
	};	
	int llen( std::string strKey ) {
		std::string __strCmd = __FUNCTION__;
		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __strCmd.c_str() ) ) )->getInt() );	
	};	
	CCRedisReply* lrange( std::string strKey, int dwBegin = 0, int dwEnd = -1 ) {
		std::ostringstream 	__osCmd;
		__osCmd << __FUNCTION__ << " " << strKey << " " << dwBegin << " " << dwEnd;
		return new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str() ) );
	};  
  //-- HASH
	CCRedisReply* hget( std::string strKey, std::string strField ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey + std::string( " " ) + strField;
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str()  ) );
	};
	CCRedisReply* hgetall( std::string strKey ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " ) + strKey;
		return new CCRedisReply( redisCommand( m_rh, __strCmd.c_str()  ) );
	};
	bool hmset( std::string strKey, const T_mMap &m ) {
		std::ostringstream 	__osCmd;
		__osCmd << "*" << m.size()*2 + 2 << "\r\n" << "$5\r\nHMSET\r\n$" << strKey.length() << "\r\n" << strKey;
		for_each( m.begin(), m.end(), F_SerializeMap( __osCmd ) );
		__osCmd << "\r\n";
		anetWrite( m_rh, const_cast<char*>( __osCmd.str().c_str() ), __osCmd.str().length() );
		return( protReply() );
	};
	bool hmset( std::string strKey, const T_vTupRedis &vTup ) {
		std::ostringstream 	__osCmd;
		__osCmd << "*" << vTup.size()*2 + 2 << "\r\n" << "$5\r\nHMSET\r\n$" << strKey.length() << "\r\n" << strKey;
		for_each( vTup.begin(), vTup.end(), F_SerializeTup( __osCmd ) );
		__osCmd << "\r\n";
		anetWrite( m_rh, const_cast<char*>( __osCmd.str().c_str() ), __osCmd.str().length() );
		return( protReply() );
	};
	
  //--
  	CCRedisReply* native( std::string strCmd, std::string strKey ) {
		std::ostringstream 	__osCmd;
		__osCmd << strCmd << " " << strKey;
		return new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str(), "" ) );
	};
	CCRedisReply* native( std::string strCmd, std::string strKey, std::string strValue ) {
		std::ostringstream 	__osCmd;
		__osCmd << strCmd << " " << strKey << " %b";
		return new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str(), strValue.c_str(), strValue.length() ) );
	};
  //-- 
  	bool expire( const std::string strKey, int dwSeconds ) {
  		std::ostringstream 	__osCmd;
  		__osCmd << __FUNCTION__ << " " << strKey << " " << dwSeconds;
  		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str() ) ) )->replyType() != ERROR ? true : false );
  	};	
  	bool exists( const std::string strKey ) {
  		std::ostringstream 	__osCmd;
  		__osCmd << __FUNCTION__ << " " << strKey;
  		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str() ) ) )->getInt() == 1 ? true : false );
  	};	
  	int del( const std::string strKey ) {
  		std::ostringstream 	__osCmd;
  		__osCmd << __FUNCTION__ << " " << strKey;
  		return SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str() ) ) )->getInt();
  	};	
  	bool rename( const std::string strKeyOld, std::string strKeyNew ) {
  		std::ostringstream 	__osCmd;
  		__osCmd << __FUNCTION__ << " " << strKeyOld << " " << strKeyNew;
  		return( SP_RedRpl( new CCRedisReply( redisCommand( m_rh, __osCmd.str().c_str() ) ) )->replyType() != ERROR ? true : false );
  	};	
  //--
	void auth( std::string strPwd ) {
		std::string __strCmd = __FUNCTION__ + std::string( " " );
		CCRedisReply __r( redisCommand( m_rh, std::string( __strCmd + strPwd ).c_str()  ) );
	};
	void connect() {
		SP_RedRpl __rep( new CCRedisReply );
		if( redisConnect( &m_rh, m_strRedisHost.c_str(), m_dwRedisPort ) )
			throw( std::runtime_error( "CCRedis-Exception: connect failed" ) );
	};

	CCRedis( const std::string strRedisHost = "127.0.0.1", int dwRedisPort = 6379 ) : m_strRedisHost( strRedisHost ), m_dwRedisPort( dwRedisPort ) {};
	virtual ~CCRedis() {}

};
typedef boost::scoped_ptr<CCRedis::CCRedisReply>	Redis_SPRepl;
typedef CCRedis::T_vString							Redis_StrVec;
typedef Redis_StrVec::iterator						Redis_itStr;
typedef Redis_StrVec::const_iterator				Redis_citStr;
typedef CCRedis::T_mMap								Redis_mHash;
typedef CCRedis::T_vTupRedis						Redis_vHash;

#endif
