CREATE KEYSPACE IF NOT EXISTS dstream
  WITH REPLICATION = {
   'class': 'SimpleStrategy','replication_factor':'1'
 } ;

CREATE KEYSPACE IF NOT EXISTS dataframe
  WITH REPLICATION = {
   'class': 'SimpleStrategy','replication_factor':'1'
 } ;

CREATE TABLE IF NOT EXISTS dstream.bots (
  ip text PRIMARY KEY
 );

CREATE TABLE IF NOT EXISTS dstream.ctr (
   ip text PRIMARY KEY,
   ctr double
 );
CREATE TABLE IF NOT EXISTS dataframe.bots (
  ip text PRIMARY KEY
 );

CREATE TABLE IF NOT EXISTS dataframe.ctr (
   ip text PRIMARY KEY,
   ctr double
 );