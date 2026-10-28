"""Defaults, CHECKs, and basic aggregate behavior against the owned C peer."""
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path
from subqueries import QueryConnection

CASES = (
    "CREATE SEQUENCE dc_seq",
    "CREATE TABLE dc_rows(id int PRIMARY KEY,a int DEFAULT 2+3 CHECK(a>0),b int DEFAULT nextval('dc_seq')+10,label varchar(12) DEFAULT lower('OK'),CONSTRAINT ordered CHECK(b>=a))",
    "INSERT INTO dc_rows(id) VALUES(1),(2) RETURNING *",
    "INSERT INTO dc_rows VALUES(3,NULL,DEFAULT,DEFAULT) RETURNING *",
    "UPDATE dc_rows SET a=DEFAULT,b=DEFAULT WHERE id=1 RETURNING *",
    "INSERT INTO dc_rows VALUES(4,-1,30,'bad')",
    "INSERT INTO dc_rows VALUES(1,-1,30,'bad') ON CONFLICT DO NOTHING",
    "INSERT INTO dc_rows VALUES(1,1,30,'x') ON CONFLICT(id) DO UPDATE SET a=-1",
    "UPDATE dc_rows SET b=1 WHERE id=1 RETURNING *",
    "SELECT * FROM dc_rows ORDER BY id",
    "ALTER TABLE dc_rows ALTER COLUMN a SET DEFAULT 7+1",
    "INSERT INTO dc_rows(id) VALUES(4) RETURNING *",
    "ALTER TABLE dc_rows ALTER a DROP DEFAULT",
    "INSERT INTO dc_rows(id) VALUES(5) RETURNING *",
    "ALTER TABLE dc_rows ADD CONSTRAINT old_bad CHECK(id<4)",
    "ALTER TABLE dc_rows ADD CONSTRAINT old_bad CHECK(id<4) NOT VALID",
    "INSERT INTO dc_rows(id) VALUES(6)",
    "UPDATE dc_rows SET label='other' WHERE id=4",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "DELETE FROM dc_rows WHERE id>=4",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT old_bad",
    "ALTER TABLE dc_rows RENAME CONSTRAINT old_bad TO limited",
    "ALTER TABLE dc_rows DROP CONSTRAINT limited",
    "ALTER TABLE dc_rows VALIDATE CONSTRAINT dc_rows_pkey",
    "ALTER TABLE dc_rows ADD CONSTRAINT dc_rows_pkey CHECK(a>0)",
    "CREATE TABLE dc_other(a int,CONSTRAINT ordered CHECK(a>0))",
    "CREATE TABLE dc_cross(a int CHECK(a<b), b int)",
    "INSERT INTO dc_cross VALUES(1,2),(NULL,1)",
    "INSERT INTO dc_cross VALUES(2,1)",
    "CREATE TABLE dc_unknown(a int CHECK(NULL))",
    "INSERT INTO dc_unknown VALUES(1)",
    "CREATE TABLE dc_reject1(a int DEFAULT a+1)",
    "CREATE TABLE dc_reject2(a int DEFAULT (SELECT 1))",
    "CREATE TABLE dc_reject3(a int DEFAULT sum(1))",
    "CREATE TABLE dc_reject4(a int DEFAULT row_number() OVER())",
    "CREATE TABLE dc_reject5(a int CHECK(1))",
    "CREATE TABLE dc_reject6(a int CHECK((SELECT true)))",
    "CREATE TABLE dc_reject7(a int CHECK(sum(a)>0))",
    "CREATE TABLE dc_reject8(a int CHECK(row_number() OVER()>0))",
    "ALTER TABLE dc_rows ALTER a SET DEFAULT true",
    "ALTER TABLE dc_rows ALTER a SET DEFAULT 'bad'",
    "ALTER TABLE dc_rows ALTER label SET DEFAULT 'long default text'",
    "INSERT INTO dc_rows(id) VALUES(8)",
    "ALTER TABLE dc_rows ALTER label SET DEFAULT 'ok'",
    "DROP SEQUENCE dc_seq",
    "CREATE TABLE dc_notvalid(a int CHECK(a>0) NOT VALID)",
    "CREATE TABLE dc_ag(g int,x int,s varchar(12))",
    "INSERT INTO dc_ag VALUES(1,1,'b'),(1,2,'a'),(1,NULL,NULL),(2,0,'z'),(2,-1,'x')",
    "SELECT count(*),count(x),min(x),max(x),avg(x),min(s),max(s) FROM dc_ag",
    "SELECT g,count(x),min(x),max(x),avg(x) FROM dc_ag GROUP BY g ORDER BY g",
    "SELECT count(x),min(x),max(x),avg(x) FROM dc_ag WHERE false",
    "SELECT count(NULL),count(false),count(0),min(NULL),max(NULL)",
    "SELECT avg(NULL)",
    "SELECT min(true),max(false)",
    "SELECT avg(s) FROM dc_ag",
    "SELECT g FROM dc_ag GROUP BY g HAVING avg(x)>0 ORDER BY max(x)",
    "SELECT x,count(x) OVER(ORDER BY x),min(x) OVER(),max(s) OVER(),avg(x) OVER(ORDER BY x) FROM dc_ag ORDER BY x,s",
    "WITH q AS MATERIALIZED(SELECT g,avg(x) AS a FROM dc_ag GROUP BY g) SELECT g,a,a::integer,a+a,a*2,a/2 FROM q ORDER BY a",
    "WITH q AS(SELECT g,avg(x) AS a FROM dc_ag GROUP BY g) SELECT sum(a),avg(a),min(a),max(a),count(a) FROM q",
    "SELECT avg(x)::integer FROM (SELECT 0 AS x UNION ALL SELECT -1) d",
    "SELECT avg(x) FROM (SELECT 9223372036854775807::bigint AS x UNION ALL SELECT -9223372036854775808::bigint) d",
    "SELECT avg(x) FROM (SELECT 9999 AS x UNION ALL SELECT 10000) d",
    "SELECT avg(x) FROM (SELECT 1 AS x UNION ALL SELECT -1) d",
    "SELECT avg(x) FROM (SELECT 1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d",
    "SELECT avg(x) FROM (SELECT -1 AS x UNION ALL SELECT 0 UNION ALL SELECT 0) d",
    "SELECT avg(x),sum(x),min(x),max(x) FROM (SELECT '1.00'::numeric AS x UNION ALL SELECT '2.0'::numeric) d",
    "SELECT DISTINCT x FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d",
    "SELECT x,count(*) FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d GROUP BY x",
    "SELECT '1.5'::numeric::integer,'-1.5'::numeric::integer,'0.5'::numeric::smallint",
    "SELECT '2147483647.5'::numeric::integer",
    "SELECT '-2147483648.5'::numeric::integer",
    "SELECT '9223372036854775807.5'::numeric::bigint",
    "SELECT avg(x)>'1' FROM (SELECT 1 AS x UNION ALL SELECT 2) d",
    "SELECT 1::numeric/3::numeric,1::numeric+2,1::numeric*2,1::numeric%2",
    "SELECT abs(ALL NULL), abs(ALL -3), sum(ALL 3)",
    "SELECT abs(DISTINCT NULL)",
    "SELECT sin(DISTINCT 'bad')",
    "SELECT sin('bad' ORDER BY 1)",
    "SELECT sin(1 ORDER BY 1 NULLS FIRST)",
    "SELECT sin(1 ORDER BY 1 USING <)",
    "SELECT sin(1 ORDER BY 1 USING OPERATOR(pg_catalog.<))",
    "SELECT sin(1 ORDER BY 1 USING OPERATOR(<))",
    "SELECT sin(1 ORDER BY 1 USING OPERATOR(a.b.<))",
    "SELECT sin(1 ORDER BY 1 ASC USING <)",
    "SELECT width_bucket(DISTINCT ARRAY[1], NULL)",
    "SELECT width_bucket(DISTINCT true, ARRAY[1])",
    "SELECT trunc(DISTINCT 'bad')",
    "SELECT sin(ALL VARIADIC 1)",
    "SELECT sin(DISTINCT VARIADIC 1)",
    "SELECT abs(NULL ORDER BY NULL)",
    "SELECT abs(DISTINCT NULL ORDER BY NULL)",
    "SELECT abs(DISTINCT 1, 2)",
    "SELECT abs(-1.230::numeric),numeric_abs(-3),sign(-2.0::numeric),ceil(-1.2::numeric),ceiling(1.2::numeric),floor(-1.2::numeric)",
    "SELECT 5 & 3, 5 | 2 # 3, 1 << 3 + 1, @ -2 + 1, ~ 1 + 2",
    "SELECT 1::int2 & 3::int4, 1::int8 | 2::int2, 1::int2 << 15, -8::int8 >> 2",
    "SELECT 1 << 31, -2147483648 >> 31, 1::int8 << 63, -9223372036854775808::int8 >> 63",
    "SELECT int2and(3::int2,1::int2),int4or(2,1),int8xor(7,3),int4not(0),int2shl(1::int2,15),int8shr(-8,2),int4abs(-3)",
    "SELECT @ NULL::int4, ~ NULL::int8, 1 & NULL::int4, NULL::int2 << 1, |/ NULL::float8, ||/ NULL::float8",
    "SELECT @ '2'",
    "SELECT ~ '2'",
    "SELECT 2 & '1', '2' << 1",
    "SELECT 5&~1",
    "SELECT 1 &-- comment\n 2, @-- comment\n -2",
    "SELECT 1 &/* comment */ 2, @/* comment */ -2",
    "SELECT 1 +-- comment\n 2, 1 +/* comment */ 2",
    "SELECT 1+-2, 6*-2, 1>=-2",
    "SELECT 5*~1",
    "SELECT 1 ^+ 2 + 3",
    "SELECT / 1",
    "SELECT ^ 2",
    "SELECT % 2",
    "SELECT != 1",
    "SELECT => 1",
    "SELECT :: 1",
    "SELECT 1<<@ 2",
    "SELECT @-2",
    "SELECT |/-9",
    "SELECT NULL & NULL",
    "SELECT NULL << NULL",
    "SELECT '2' << 1::int2",
    "SELECT 'bad' << 1::int8",
    "SELECT 'bad' & true",
    "SELECT |/ 9::numeric, ||/ -8::float8, @ -1.25::numeric, |/ 'Infinity'::float8, ||/ 'NaN'::float8",
    "SELECT @ -2147483648::int4",
    "SELECT |/ -1",
    "SELECT 1 << 1::int8",
    "SELECT 1.0 & 1",
    "SELECT ~ true",
    "SELECT round(1.25::numeric,1),round(-1.25::numeric,1),trunc(-1.29::numeric,1),round(1450::numeric,-2),trunc(-1450::numeric,-2)",
    "SELECT round(1::numeric,-2147483648),trunc(1::numeric,-2147483648),length(round(1::numeric,2147483647)::text)",
    "SELECT round('NaN'::numeric),trunc('Infinity'::numeric),abs('-Infinity'::numeric),sign('NaN'::numeric)",
    "SELECT abs((-32768)::smallint)",
    "SELECT abs((-2147483648)::integer)",
    "SELECT abs((-9223372036854775808)::bigint)",
    "SELECT abs((-2)::smallint),abs((-2)::integer),abs((-2)::bigint),abs((-2)::real),abs((-2)::float8)",
    "SELECT round(2.5::float8),round(3.5::float8),trunc((-1.9)::float8),ceil((-0.2)::float8),floor((-0.2)::float8)",
    "SELECT sign('NaN'::float8),sign('NaN'),round('Infinity'::float8),abs('-Infinity'::float8)",
    "SELECT float4abs('-2.5'),float8abs(-2),dround(2.5),dtrunc(-1.9),dsqrt(9),dexp(0),dlog1(1),dlog10(100)",
    "SELECT float4abs(NULL),float8abs('-Infinity'),dround('NaN'),dtrunc('-0'),dsqrt(NULL),dlog10('Infinity')",
    "SELECT float4abs(1::float8)",
    "SELECT dsqrt(true)",
    "SELECT dsqrt(-1)",
    "SELECT dlog1(0)",
    "SELECT DISTINCT dround(x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY round(x)",
    "SELECT DISTINCT round(x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY dround(x)",
    "SELECT float4abs(1.234567890123456789::numeric),float4abs('1.234567890123456789')",
    "SELECT float4abs(NULL),float8abs(NULL),dround(NULL),dtrunc(NULL),dsqrt(NULL),dexp(NULL),dlog1(NULL),dlog10(NULL)",
    "SELECT dsqrt('4'),dround('2.5'),dlog10('100')",
    "SELECT dsqrt('4'::text)",
    "SELECT float4abs()",
    "SELECT dround(1,2)",
    "SELECT dexp(1000)",
    "SELECT dexp(-1000)",
    "SELECT round(1::integer,2::smallint),numeric_abs(1::bigint),round(NULL::numeric,1),round(1::numeric,NULL::integer)",
    "SELECT width_bucket(-1,0,10,5),width_bucket(0,0,10,5),width_bucket(2,0,10,5),width_bucket(10,0,10,5)",
    "SELECT width_bucket(11,10,0,5),width_bucket(10,10,0,5),width_bucket(8,10,0,5),width_bucket(0,10,0,5)",
    "SELECT width_bucket(0.3::numeric,0::numeric,1::numeric,10),width_bucket('NaN'::numeric,0,1,3),width_bucket(NULL::numeric,0,1,3)",
    "SELECT width_bucket(0::float8,'-1e308'::float8,'1e308'::float8,10),width_bucket('NaN'::float8,0,1,3)",
    "SELECT width_bucket('Infinity'::numeric,0,1,3),width_bucket('-Infinity'::numeric,0,1,3),width_bucket('Infinity'::float8,0,1,3),width_bucket('-Infinity'::float8,0,1,3)",
    "SELECT width_bucket(0,'-Infinity'::numeric,1,3)",
    "SELECT width_bucket(0,0,'Infinity'::numeric,3)",
    "SELECT width_bucket(0::float8,'-Infinity'::float8,1,3)",
    "SELECT width_bucket(0::float8,0,'Infinity'::float8,3)",
    "SELECT width_bucket(0.299999999999999999999999999999999999,0::numeric,1,10),width_bucket(0.300000000000000000000000000000000001,0::numeric,1,10)",
    "SELECT width_bucket(0.299999999999999999999999999999999999,1::numeric,0,10),width_bucket(0.300000000000000000000000000000000001,1::numeric,0,10)",
    "SELECT width_bucket(1,0,1,0)",
    "SELECT width_bucket(1,1,1,2)",
    "SELECT width_bucket(1,'NaN'::numeric,1,2)",
    "SELECT width_bucket(1,0,1,2147483647)",
    "SELECT width_bucket('b'::text,ARRAY['a','b','b','c']),width_bucket('z'::text,ARRAY[]::text[])",
    "SELECT width_bucket('b'::varchar,ARRAY['a','b']),width_bucket('b'::name,ARRAY['a','b']),width_bucket('b'::text,'{a,b}'),width_bucket('b'::text,NULL)",
    "SELECT width_bucket('b','{a,b}'),width_bucket(NULL,NULL)",
    "SELECT width_bucket('a'::text,ARRAY['a',NULL])",
    "SELECT width_bucket(2,ARRAY[1,2,2,4]),width_bucket(2.5,ARRAY[1,2,3]),width_bucket(1,ARRAY[]::int4[])",
    "SELECT width_bucket(2,'[0:2]={1,2,3}'::int4[]),width_bucket('NaN'::float8,ARRAY[1::float8,'NaN'::float8])",
    "SELECT width_bucket(2::bigint,ARRAY[1::int4,2::int4]),width_bucket(2::numeric,ARRAY[1::int4,2::int4])",
    "SELECT ARRAY[1,2.5],ARRAY[1,2]::bigint[],'[0:2]={1,2,3}'::int4[]",
    "SELECT '{{1,2},{3,4}}'::int4[],array_length('{{1,2},{3,4}}'::int4[],2)",
    "SELECT width_bucket(1,'{{1,2},{3,4}}'::int4[])",
    "SELECT width_bucket(1,ARRAY[1,NULL])",
    "SELECT ARRAY[1,'1.5']::numeric[],ARRAY['1','2']::int4[],ARRAY[ARRAY[],ARRAY[]]::int4[][]",
    "SELECT ARRAY[NULL,ARRAY[1]]",
    "SELECT ARRAY[ARRAY[1],NULL]",
    "SELECT ARRAY(SELECT 1),ARRAY(SELECT 1 WHERE false),width_bucket(1,ARRAY(SELECT 1))",
    "SELECT ARRAY(SELECT 'a')::text[],ARRAY(SELECT 1)::bigint[],ARRAY(SELECT 1)=ARRAY[1]",
    "SELECT '{{},{}}'::int4[], '[3]={1,2,3}'::int4[], '[0:1] [2:3] = {{1,2},{3,4}}'::int4[]",
    "SELECT width_bucket(1,ARRAY['a'])",
    "SELECT width_bucket('bad', ARRAY[1,2])",
    "SELECT ARRAY[1]+1",
    "SELECT -ARRAY[1]",
    "SELECT array_agg(x),array_length(array_agg(x),1),width_bucket('b'::text,array_agg(x)) FROM (SELECT 'a'::text AS x UNION ALL SELECT 'b'::text) t",
    "SELECT '[2:1]={}'::int4[]",
    "SELECT '[2147483648:2147483649]={1,2}'::int4[]",
    "SELECT '[1:1][1:1][1:1][1:1][1:1][1:1][1:1]={{{{{{{1}}}}}}}'::int4[]",
    "SELECT '{{1},{2,3}}'::int4[]",
    "SELECT ARRAY[ARRAY[1],ARRAY[2,3]]",
    "SELECT ARRAY[ARRAY[]::int4[],ARRAY[1]]",
    "SELECT array_agg(x) OVER (ORDER BY x) FROM (SELECT 'a'::text AS x UNION ALL SELECT 'b'::text) t",
    "SELECT '[2147483647:2147483647]={1}'::int4[]",
    "SELECT '[ 1 : 2 ]={1,2}'::int4[]",
    "SELECT ARRAY[[[[[[[1]]]]]]]",
    "SELECT ARRAY[1.26]::numeric(3,1)[], '{1.26,2.34}'::numeric(3,1)[]",
    "SELECT '{1234}'::numeric(3,1)[]",
    "SELECT ARRAY[1]=ARRAY[1]::bigint[],ARRAY[1]=ARRAY[1.0]::numeric[]",
    "SELECT '{{{{{{{}}}}}}}'::int4[]",
    "SELECT ARRAY[ARRAY[[[[[[1]]]]]],ARRAY[[[[[1]]]]]]",
    "SELECT ARRAY[ARRAY[[[[[1]]]]],ARRAY[[[[[[1]]]]]]]",
    "CREATE SEQUENCE dc_math_fold_seq",
    "CREATE TABLE dc_math_fold(a bigint DEFAULT nextval('dc_math_fold_seq') + abs('-9223372036854775808'::bigint))",
    "INSERT INTO dc_math_fold DEFAULT VALUES",
    "SELECT currval('dc_math_fold_seq')",
    "SELECT sqrt(0::numeric),sqrt(2::numeric),sqrt(4.00::numeric),numeric_sqrt(9),sqrt(NULL::numeric)",
    "SELECT pi(),degrees(pi()),radians(180),cbrt(-8),dcbrt(-8),sin(0),cos(0),tan(0),cot(0)",
    "SELECT sind(30),sind(180),sind(270),cosd(60),cosd(180),tand(45),tand(90),cotd(45),cotd(0)",
    "SELECT asind(-0.5),acosd(-0.5),atand(1),atan2d(1,-1),atan2d(-1,-1)",
    "SELECT atan2('-0'::float8,'-0'::float8),atan2d('-0'::float8,'-0'::float8)",
    "SELECT asind('-0'::float8),sind('-0'::float8),cotd('-0'::float8),radians('1e-320'::float8)",
    "SELECT asin(0.5),acos(0.5),atan(1),atan2(1,-1),degrees(1),radians(1)",
    "SELECT atanh(-1),atanh(1),acosh(1),asinh('-Infinity'::float8),cbrt('-Infinity'::float8)",
    "SELECT asind(2)",
    "SELECT cotd('Infinity'::float8)",
    "SELECT sinh(1),cosh(1),tanh(1),asinh(1),acosh(2),atanh(0.5)",
    "SELECT sinh(1000),cosh(1000),tanh('Infinity'::float8),asinh('-Infinity'::float8),sin('NaN'::float8),sin(NULL)",
    "SELECT asin(2)",
    "SELECT acosh(0)",
    "SELECT atanh(2)",
    "SELECT cos('Infinity'::float8)",
    "SELECT sqrt(9999::numeric),sqrt(10000::numeric),sqrt(99999999::numeric),sqrt(100000000::numeric)",
    "SELECT sqrt(0.0001::numeric),sqrt(0.00009999::numeric),sqrt('1e-100'::numeric),sqrt('1e100'::numeric)",
    "SELECT sqrt('2.2500000000000000000000000000000001'::numeric),sqrt('2.2499999999999999999999999999999999'::numeric)",
    "SELECT sqrt('NaN'::numeric),sqrt('Infinity'::numeric),sqrt('NaN'::float8),sqrt('Infinity'::float8)",
    "SELECT sqrt('-Infinity'::numeric)",
    "SELECT sqrt((-1)::numeric)",
    "SELECT sqrt((-1)::float8)",
    "SELECT sqrt('4'),sqrt(4),sqrt(4::real),sqrt(4::float8)",
    "SELECT length(sqrt('1e-16383'::numeric)::text),length(sqrt('1e131071'::numeric)::text)",
    "CREATE SEQUENCE dc_sqrt_fold_seq",
    "CREATE TABLE dc_sqrt_fold(a bigint DEFAULT nextval('dc_sqrt_fold_seq') + sqrt((-1)::numeric))",
    "INSERT INTO dc_sqrt_fold DEFAULT VALUES",
    "SELECT currval('dc_sqrt_fold_seq')",
    *(f"SELECT sqrt('{((2 * 10**1000 + 1)**2 * 25) + delta}e-2002'::numeric)"
      for delta in (-1, 0, 1)),
    "SELECT sqrt('-0'::float8),sqrt('5e-324'::float8),sqrt('1.7976931348623157e308'::float8)",
    "SELECT sqrt('-Infinity'::float8)",
    "SELECT exp(0::numeric),exp(1::numeric),exp((-1)::numeric),exp(10.000::numeric),numeric_exp(1)",
    "SELECT exp(0.01::numeric),exp(0.0100001::numeric),exp((-0.01)::numeric),exp((-0.0100001)::numeric)",
    "SELECT exp('1e-30'::numeric),exp('2.302585092994045684'::numeric),exp((-10.000)::numeric)",
    "SELECT exp('NaN'::numeric),exp('Infinity'::numeric),exp('-Infinity'::numeric),exp(NULL::numeric)",
    "SELECT exp(4),exp('4'),exp(1::real),exp('NaN'::float8),exp('Infinity'::float8),exp('-Infinity'::float8)",
    "SELECT exp(6000::numeric)",
    "SELECT exp((-6000)::numeric)",
    "SELECT exp((-100)::numeric),exp((-200)::numeric),exp((-1000)::numeric)",
    "SELECT exp((-170)::numeric),exp((-180)::numeric),exp((-2300)::numeric),exp((-2302)::numeric),exp((-2303)::numeric)",
    "SELECT exp('1e-16383'::numeric),exp('-1e-16383'::numeric)",
    "SELECT exp(1000::float8)",
    "SELECT exp((-1000)::float8)",
    "SELECT exp((-710)::float8)",
    "CREATE SEQUENCE dc_exp_fold_seq",
    "CREATE TABLE dc_exp_fold(a bigint DEFAULT nextval('dc_exp_fold_seq') + exp(6000::numeric))",
    "INSERT INTO dc_exp_fold DEFAULT VALUES",
    "SELECT currval('dc_exp_fold_seq')",
    "SELECT ln(1::numeric),ln(2::numeric),numeric_ln(3),ln('Infinity'::numeric),ln('NaN'::numeric)",
    "SELECT ln(0::numeric)",
    "SELECT ln((-1)::numeric)",
    "SELECT ln('-Infinity'::numeric)",
    "SELECT ln(0::float8)",
    "SELECT ln((-1)::float8)",
    "SELECT ln(2),ln('2'),ln(2::real),ln(2::float8),ln('Infinity'::float8),ln('NaN'::float8)",
    "SELECT ln(0.9::numeric),ln(1.1::numeric),ln(0.9001::numeric),ln(1.0999::numeric)",
    "SELECT ln('1.000000000000000000000000000000000000001'::numeric),ln('0.999999999999999999999999999999999999999'::numeric)",
    "SELECT ln('1e-1000'::numeric),ln('1e1000'::numeric)",
    "SELECT log(100::numeric),log10(1000::numeric),log(2::numeric,8::numeric),numeric_log(3,27)",
    "SELECT log(1::numeric,2::numeric)",
    "SELECT log(0::numeric,(-1)::numeric)",
    "SELECT log('Infinity'::numeric,'Infinity'::numeric),log('Infinity'::numeric,2::numeric),log(0.5::numeric,'Infinity'::numeric)",
    "SELECT log(1::numeric,'Infinity'::numeric)",
    "SELECT log(2::float8),log10(2::float8),log('NaN'::float8),log('Infinity'::float8)",
    f"SELECT ln('1.{'0' * 16382}1'::numeric),ln('0.{'9' * 16383}'::numeric)",
    f"SELECT log('1.{'0' * 1000}1'::numeric,2::numeric),log('1.{'0' * 1000}1'::numeric,'1.{'0' * 1002}1'::numeric)",
    "CREATE SEQUENCE dc_ln_fold_seq",
    "CREATE TABLE dc_ln_fold(a bigint DEFAULT nextval('dc_ln_fold_seq') + ln((-1)::numeric))",
    "INSERT INTO dc_ln_fold DEFAULT VALUES",
    "SELECT currval('dc_ln_fold_seq')",
    "SELECT 2^3^2, -2^2, 2*3^2,2^(-3),2.0^3,2::real^3::real",
    "SELECT 2^-1",
    "SELECT 2^+3",
    "SELECT 2^*3",
    "SELECT 2^^3",
    "SELECT 2^?3",
    "SELECT 2^`3",
    "SELECT 2^/*comment*/3",
    "SELECT 2 ^ -1, 2 ^ +3",
    "SELECT DISTINCT x^2 FROM (SELECT 2 AS x UNION ALL SELECT 3) t ORDER BY power(x,2)",
    "SELECT DISTINCT power(x,2) FROM (SELECT 2 AS x UNION ALL SELECT 3) t ORDER BY x^2",
    "SELECT power(2,3),pow(2::numeric,3::numeric),numeric_power(2,3),dpow(2,3)",
    "SELECT power(0::numeric,0::numeric),power(2.00::numeric,0.0::numeric),power((-2)::numeric,3::numeric),power((-2)::numeric,4::numeric)",
    "SELECT power(2::numeric,(-3)::numeric),power(9::numeric,0.5::numeric),power(0::numeric,0.5::numeric)",
    "SELECT power(0::numeric,(-1)::numeric)",
    "SELECT power((-2)::numeric,0.5::numeric)",
    "SELECT power('NaN'::numeric,0),power(1::numeric,'NaN'::numeric),power('NaN'::numeric,2)",
    "SELECT power((-1)::numeric,'Infinity'::numeric),power(0.5::numeric,'Infinity'::numeric),power(2::numeric,'-Infinity'::numeric)",
    "SELECT power('-Infinity'::numeric,3),power('-Infinity'::numeric,4),power('-Infinity'::numeric,-3)",
    "SELECT power('-Infinity'::numeric,0.5::numeric)",
    "SELECT power(1::numeric,2147483648::numeric),power((-1)::numeric,2147483649::numeric),power(2::numeric,2147483648::numeric)",
    "SELECT power(2::numeric,-2147483648::numeric),power(2::numeric,10000::numeric)",
    "SELECT power('NaN'::float8,0::float8),power(1::float8,'NaN'::float8),power('-Infinity'::float8,-3::float8)",
    "SELECT power(0::float8,(-1)::float8)",
    "SELECT power((-2)::float8,0.5::float8)",
    *(f"SELECT power('{base}.125'::numeric,'{exponent}'::numeric)" for base in range(1, 12) for exponent in (-7,-2,0,1,2,3,7)),
    *(f"SELECT power('{base}.12345'::numeric,'{exponent}.25'::numeric)" for base in range(1, 12) for exponent in (-7,-2,0,1,2,7)),
    *(f"SELECT power('{base}'::numeric,{exponent}::numeric)" for base in ('0.0001','0.001','0.01','0.1','1','10','100','1000','10000','1.0000','100.0100') for exponent in (-5,-1,0,1,2,3,5)),
    "SELECT power(2::numeric,'3'),power('2',3::numeric),power(2::float8,3::numeric),power(NULL::numeric,2),power(2,NULL)",
    "SELECT power('1.00000000000000000000000000000000001'::numeric,2147483648::numeric),power(2::numeric,'1e-1000'::numeric)",
    "SELECT power('1e-1000'::numeric,0.5::numeric),power('1e1000'::numeric,0.5::numeric)",
    "CREATE SEQUENCE dc_power_fold_seq",
    "CREATE TABLE dc_power_fold(a bigint DEFAULT nextval('dc_power_fold_seq') + power(0::numeric,-1::numeric))",
    "INSERT INTO dc_power_fold DEFAULT VALUES",
    "SELECT currval('dc_power_fold_seq')",
    "SELECT scale(1.2300::numeric),min_scale(1.2300::numeric),trim_scale(1.2300::numeric)",
    "SELECT scale(0.0000::numeric),min_scale(0.0000::numeric),trim_scale(0.0000::numeric)",
    "SELECT scale('NaN'::numeric),min_scale('Infinity'::numeric),trim_scale('-Infinity'::numeric)",
    "SELECT scale(1),min_scale('1.200'),trim_scale('001.200'),scale(NULL::numeric)",
    "SELECT scale(1::float8)",
    "SELECT div(7::numeric,2::numeric),div((-7)::numeric,2::numeric),numeric_div_trunc(7,2),mod((-7),3)",
    "SELECT mod(1::smallint,2::smallint),mod(5::bigint,3::integer),mod(5::integer,2::smallint)",
    "SELECT mod('7','3')",
    "SELECT mod('7',3),mod(7,'3'),mod(7.50::numeric,2),numeric_mod(7.50,2)",
    "SELECT numeric_add(1.20,2),numeric_sub(1,2),numeric_mul(1.20,2),numeric_div(1,2)",
    "SELECT numeric_eq('NaN','NaN'),numeric_ne(1,2),numeric_gt('Infinity',1),numeric_ge(1,1),numeric_lt('-Infinity','NaN'),numeric_le(1,1),numeric_cmp('NaN','Infinity')",
    "SELECT numeric_inc(1.20),numeric_inc('Infinity'),numeric_uminus('-Infinity'),numeric_uplus(1.00)",
    "SELECT numeric_smaller(1.0,1.000),numeric_larger(1.0,1.000),numeric_smaller('NaN','Infinity'),numeric_larger('-Infinity',0)",
    "SELECT numeric_eq(NULL,1),numeric_cmp(1,NULL),numeric_inc(NULL),numeric_smaller(NULL,1),numeric_uminus(NULL)",
    "SELECT numeric_eq(1::float8,1),numeric_inc(1::float8)",
    # PostgreSQL numeric.sql division and carry/borrow regression cases.
    "SELECT 999999999999999999999::numeric/1000000000000000000000,div(999999999999999999999::numeric,1000000000000000000000),mod(999999999999999999999::numeric,1000000000000000000000)",
    "SELECT div(-9999999999999999999999::numeric,1000000000000000000000),mod(-9999999999999999999999::numeric,1000000000000000000000),div(-9999999999999999999999::numeric,1000000000000000000000)*1000000000000000000000+mod(-9999999999999999999999::numeric,1000000000000000000000)",
    "SELECT 12345678901234567890::numeric/123,div(12345678901234567890::numeric,123),mod(12345678901234567890::numeric,123)",
    "SELECT 9999.9999::numeric+0.0001::numeric,10000.0000::numeric-0.0001::numeric,9999.9999::numeric*9999.9999::numeric",
    *(f"SELECT {a}::numeric/{b}::numeric,div({a}::numeric,{b}::numeric),mod({a}::numeric,{b}::numeric),div({a}::numeric,{b}::numeric)*{b}::numeric+mod({a}::numeric,{b}::numeric)" for a in ('-10000.0001','0.00001','9999.9999') for b in ('-0.003','3.00','10000.0000')),
    "SELECT div('NaN'::numeric,0),div('Infinity'::numeric,'Infinity'::numeric),div(2::numeric,'Infinity'::numeric)",
    "SELECT mod('NaN'::numeric,0),mod(1.230::numeric,'Infinity'::numeric),mod('Infinity'::numeric,2)",
    "SELECT div('Infinity'::numeric,0)",
    "SELECT mod('Infinity'::numeric,0)",
    "SELECT numeric_div(NULL::numeric,0::numeric),div(NULL::numeric,0::numeric)",
    "SELECT mod(1::float8,2::float8)",
    "CREATE SEQUENCE dc_div_fold_seq",
    "CREATE TABLE dc_div_fold(a bigint DEFAULT nextval('dc_div_fold_seq') + div(1::numeric,0::numeric))",
    "INSERT INTO dc_div_fold DEFAULT VALUES",
    "SELECT currval('dc_div_fold_seq')",
    "SELECT gcd(24,18),lcm(-24,18),gcd(1.20::numeric,0.45::numeric),lcm(1.20::numeric,0.45::numeric)",
    "SELECT gcd(0.000::numeric,0::numeric),lcm(0.000::numeric,2::numeric),gcd('Infinity'::numeric,0),lcm('NaN'::numeric,0)",
    "SELECT gcd((-2147483648)::integer,0)",
    "SELECT gcd((-2147483648)::integer,-1),lcm((-2147483648)::integer,0)",
    "SELECT lcm((-2147483648)::integer,1)",
    "SELECT gcd((-9223372036854775808)::bigint,0)",
    "SELECT gcd(2::smallint,3::smallint)",
    "SELECT gcd(2::smallint,'3')",
    "SELECT lcm(2::bigint,3::integer),gcd('2',3),gcd(NULL::integer,2)",
    "SELECT gcd('2','3')",
    "SELECT factorial(0),factorial(1),factorial(10),factorial(20::bigint),factorial(NULL::bigint)",
    "SELECT factorial(-1)",
    "SELECT factorial(32178)",
    "SELECT length(factorial(32177)::text)",
    "SELECT factorial(1.0::numeric)",
    *(f"SELECT gcd('{a}.25'::numeric,'{b}.125'::numeric),lcm('{a}.25'::numeric,'{b}.125'::numeric)" for a in range(-4,5) for b in (-3,0,4)),
    "WITH q AS (SELECT mod(15,4) AS n) SELECT n FROM q",
    "SELECT scale('1.00000000000000000000'::numeric),min_scale('1.00000000000000000000'::numeric),trim_scale('1.00000000000000000000'::numeric)",
    f"SELECT min_scale('1.{'0' * 16383}'::numeric),trim_scale('1.{'0' * 16383}'::numeric)",
    "CREATE TABLE dc_power_expr (a numeric DEFAULT 2::numeric^3::numeric CHECK(a<3::numeric^2::numeric))",
    "INSERT INTO dc_power_expr DEFAULT VALUES RETURNING a",
    "INSERT INTO dc_power_expr VALUES(10)",
    "SELECT length((10::numeric^131071::numeric)::text)",
    "SELECT 10::numeric^131072::numeric",
    f"SELECT power('1.{'0' * 399}1'::numeric,0.5::numeric),power('0.{'9' * 400}'::numeric,2147483648::numeric)",
    *(f"SELECT power('{base}'::float8,'{exponent}'::float8)" for base in ('-Infinity','-10','-1','-0','0','0.01','1','2','Infinity','NaN') for exponent in ('-Infinity','-3','-0.5','0','0.5','3','Infinity','NaN')),
    *(f"SELECT ln('{i}.{(i * 7919) % 1000000:06d}'::numeric)" for i in range(1, 31)),
    *(f"SELECT exp('{sign}{i}.{(i * 7919) % 1000000:06d}'::numeric)"
      for sign in ('', '-') for i in range(0, 25)),
    "SELECT length((('1e131071'::numeric)*10)::text)",
    "SELECT length((('1e-10000'::numeric)*('1e-10000'::numeric))::text)",
    "CREATE TABLE dc_named(a int CONSTRAINT named_pk PRIMARY KEY, b int CONSTRAINT named_u UNIQUE, CONSTRAINT named_check CHECK(a>0))",
    "ALTER TABLE dc_named RENAME CONSTRAINT named_pk TO named_check",
    "ALTER TABLE dc_named RENAME CONSTRAINT named_check TO named_u",
    "ALTER TABLE dc_named ADD CONSTRAINT named_check UNIQUE(b)",
    "CREATE UNIQUE INDEX dc_standalone ON dc_named(b)",
    "ALTER TABLE dc_named ADD CONSTRAINT named_check UNIQUE USING INDEX dc_standalone",
    "ALTER TABLE dc_named ADD CONSTRAINT dc_standalone CHECK(b>0)",
    "CREATE TABLE dc_qualified(a int CHECK(other.a>0))",
    "CREATE TABLE dc_validqual(a int CHECK(dc_validqual.a>0))",
    "SELECT sum('1.0'::numeric),sum('1.00'::numeric),min('1.0'::numeric),min('1.00'::numeric)",
    "SELECT DISTINCT '1.0'::numeric ORDER BY '1.00'::numeric",
    "CREATE TABLE dc_decimal(a int DEFAULT '1.5'::numeric CHECK(a<'4.25'::numeric))",
    "INSERT INTO dc_decimal DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_decimal ADD CONSTRAINT decimal_later CHECK(a>'0.50'::numeric) NOT VALID",
    "INSERT INTO dc_decimal VALUES(5)",
    "CREATE SEQUENCE dc_order_seq",
    "CREATE TABLE dc_order(a int NOT NULL,CONSTRAINT z_last CHECK(nextval('dc_order_seq')>0),CONSTRAINT a_first CHECK(a>0))",
    "INSERT INTO dc_order VALUES(-1)",
    "SELECT currval('dc_order_seq')",
    "INSERT INTO dc_order VALUES(NULL)",
    "SELECT currval('dc_order_seq')",
    "INSERT INTO dc_order VALUES(1)",
    "SELECT currval('dc_order_seq')",
    "ALTER TABLE dc_order ADD CONSTRAINT volatile_validation CHECK(nextval('dc_order_seq')>0)",
    "SELECT currval('dc_order_seq')",
    "CREATE TABLE dc_default_notnull(a int DEFAULT 4 NOT NULL,b int DEFAULT 5 CHECK(b>0))",
    "INSERT INTO dc_default_notnull DEFAULT VALUES RETURNING *",
    "CREATE TABLE dc_bad_default_in(a int DEFAULT 1 IN (1,2))",
    "SELECT min('x'::name),max('x'::name),min('1'::oid),max('1'::oid)",
    "SELECT min(x),max(x) FROM (SELECT '1.0'::numeric AS x UNION ALL SELECT '1.00'::numeric) d",
    "SELECT '0e200001'::numeric",
    "SELECT '\v1\v'::numeric",
    "SELECT length(avg(x)::text) FROM (SELECT '9e131071'::numeric AS x UNION ALL SELECT '9e131071'::numeric) d",
    "SELECT length(sum(x)::text) FROM (SELECT '9e131071'::numeric AS x UNION ALL SELECT '9e131071'::numeric) d",
    "CREATE TABLE dc_fold(id int DEFAULT nextval('dc_seq'),v varchar(3) DEFAULT 'long')",
    "INSERT INTO dc_fold DEFAULT VALUES",
    "INSERT INTO dc_fold(id) SELECT 2 WHERE false",
    "UPDATE dc_fold SET v=DEFAULT WHERE false",
    "INSERT INTO dc_fold(v) VALUES(NULL) RETURNING *",
    "ALTER TABLE dc_fold ALTER v SET DEFAULT 'ok'",
    "CREATE TABLE dc_math(v int DEFAULT (nextval('dc_seq')+1/0))",
    "INSERT INTO dc_math DEFAULT VALUES",
    "INSERT INTO dc_math SELECT 1 WHERE false",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN false THEN 1/0 ELSE 7 END",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN nextval('dc_seq')>0 THEN 7 ELSE 1/0 END",
    "INSERT INTO dc_math DEFAULT VALUES",
    "ALTER TABLE dc_math ALTER v SET DEFAULT COALESCE(7,1/0)",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "ALTER TABLE dc_math ALTER v SET DEFAULT CASE WHEN nextval('dc_seq')>0 AND false THEN 9 ELSE 7 END",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "ALTER TABLE dc_math ALTER v SET DEFAULT length(substr(NULL,nextval('dc_seq')::int))",
    "INSERT INTO dc_math DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "INSERT INTO dc_rows(id) VALUES(1) ON CONFLICT(id) DO UPDATE SET label=DEFAULT WHERE false",
    "CREATE TABLE dc_plan_order(a int PRIMARY KEY DEFAULT 1/0,b varchar(1) DEFAULT 'long')",
    "INSERT INTO dc_plan_order(a) VALUES(1) ON CONFLICT(a) DO UPDATE SET a=DEFAULT",
    "INSERT INTO dc_plan_order(a,b) VALUES(1,DEFAULT),(2,DEFAULT) ON CONFLICT(a) DO UPDATE SET a=DEFAULT",
    "CREATE TABLE dc_strict(a int DEFAULT nextval('dc_seq'),b varchar(1) DEFAULT COALESCE(NULL::text || nextval('dc_seq')::text,'long'))",
    "INSERT INTO dc_strict DEFAULT VALUES",
    "ALTER TABLE dc_strict ALTER b SET DEFAULT (NULL::text || (1/0)::text)",
    "INSERT INTO dc_strict DEFAULT VALUES",
    "ALTER TABLE dc_strict ALTER b SET DEFAULT 'ok'",
    "CREATE TABLE dc_nullseq(a int DEFAULT setval('dc_seq',NULL::bigint),b int DEFAULT nextval(NULL))",
    "INSERT INTO dc_nullseq DEFAULT VALUES RETURNING *",
    "SELECT currval('dc_seq')",
    "CREATE TABLE dc_value_order(a int DEFAULT 1/0,b varchar(1) DEFAULT 'long')",
    "INSERT INTO dc_value_order(b,a) VALUES(DEFAULT,DEFAULT)",
    "INSERT INTO dc_value_order(b,a) VALUES(DEFAULT,DEFAULT),(DEFAULT,DEFAULT)",
    "INSERT INTO dc_value_order(b) VALUES(DEFAULT),(DEFAULT)",
    "INSERT INTO dc_value_order(a) VALUES(DEFAULT),(DEFAULT)",
    "CREATE TABLE dc_array_fold(a int DEFAULT nextval('dc_seq'),b varchar(1) DEFAULT COALESCE(array_length(NULL::text[],nextval('dc_seq')::int)::text,'long'))",
    "INSERT INTO dc_array_fold DEFAULT VALUES",
    "CREATE TABLE dc_admin_fold(a int DEFAULT (pg_cancel_backend(NULL::int))::int)",
    "INSERT INTO dc_admin_fold DEFAULT VALUES RETURNING *",
    "CREATE TABLE dc_checknull(a int CHECK(setval('dc_seq',NULL::bigint)>0))",
    "INSERT INTO dc_checknull VALUES(1)",
    "SELECT .5,1.,1_0.2_0e-1,-0.00,1.5+2,1.5::integer",
    # Quoted input exercises numeric_in rather than the SQL numeric lexer.
    *(f"SELECT '{value}'::numeric" for value in ('12_000.123_456', '23_000_000_000e-1_0', '.000_000_000_123e+1_1', '0b10001110111100111100001001010', '-0B_1010_1011', '+0o112402761777', '-0x0000000deadbeef', '0X_30b1_F33a')),
    *(f"SELECT '{value}'::numeric" for value in ('_123', '123_', '12__34', '123_.456', '123._456', '1.2e_34', '1.2e34_', '0b1112', '0o12345678', '0x1eg', '0x12.34', '0x__1234', '0x1234_', '0x12__34')),
    f"SELECT length(('0x{'f' * 108853}'::numeric)::text)",
    f"SELECT '0x{'f' * 110000}g'::numeric",
    "SELECT 1.245::numeric(5,2),(-1.245)::numeric(5,2),1500::numeric(2,-3),0.00123::numeric(2,4)",
    "SELECT 9.995::numeric(3,2)",
    "SELECT 99500::numeric(2,-3)",
    "SELECT 0.00999::numeric(2,4)",
    "CREATE TABLE dc_numeric(id int PRIMARY KEY,n numeric(6,2) DEFAULT 1.25 UNIQUE)",
    "INSERT INTO dc_numeric(id,n) VALUES(1,2.345),(2,-1.1),(3,10) RETURNING *",
    "INSERT INTO dc_numeric(id) VALUES(4) RETURNING *",
    "UPDATE dc_numeric SET n=8.765 WHERE id=4 RETURNING *",
    "INSERT INTO dc_numeric VALUES(5,2.350)",
    "SELECT n FROM dc_numeric WHERE n=2.350",
    "CREATE TABLE dc_numeric_raw(n numeric)",
    "INSERT INTO dc_numeric_raw VALUES(1.00),(1.0),(2.50)",
    "ANALYZE dc_numeric_raw",
    "CREATE INDEX dc_numeric_raw_idx ON dc_numeric_raw(n)",
    "SELECT n,count(*) FROM dc_numeric_raw GROUP BY n ORDER BY n",
    "SELECT 'NaN'::numeric, 'inf'::numeric, '-Infinity'::numeric",
    "SELECT '+NaN'::numeric",
    "SELECT 'Infinity'::numeric(2)",
    "SELECT 'NaN'::numeric(2)",
    "SELECT 'NaN'::numeric/0, 'NaN'::numeric%0, 'Infinity'::numeric-'Infinity'::numeric",
    "SELECT 1.230/'Infinity'::numeric,1.230%'Infinity'::numeric,'Infinity'::numeric*0",
    "SELECT '-Infinity'::numeric<0,'Infinity'::numeric<'NaN'::numeric,'NaN'::numeric='NaN'::numeric",
    "SELECT 'Infinity'::numeric/0",
    "SELECT 'NaN'::numeric::integer",
    "CREATE TABLE dc_numeric_special(n numeric UNIQUE)",
    "INSERT INTO dc_numeric_special VALUES('-Infinity'),(0),('Infinity'),('NaN')",
    "INSERT INTO dc_numeric_special VALUES('NaN')",
    "SELECT n FROM dc_numeric_special ORDER BY n",
    "SELECT min(n),max(n),sum(n),avg(n) FROM dc_numeric_special",
    "CREATE TABLE dc_smallint(id int PRIMARY KEY,n smallint UNIQUE)",
    "INSERT INTO dc_smallint VALUES(1,-32768),(2,32767),(3,0) RETURNING *",
    "INSERT INTO dc_smallint VALUES(4,32768)",
    "INSERT INTO dc_smallint VALUES(4,0)",
    "SELECT n FROM dc_smallint WHERE n=32767",
    "UPDATE dc_smallint SET n=-1 WHERE id=3 RETURNING *",
    "SELECT sum(n),avg(n),min(n),max(n) FROM dc_smallint",
    "SELECT '1.25'::real,'-0'::double precision,'Infinity'::float8,'NaN'::float4",
    "SELECT 2.5::float8::int,3.5::float8::int,(-2.5)::float8::int",
    "SELECT '0.0001'::float8,'0.00001'::float8,'1000000'::float4,'1000000000000000'::float8",
    "SELECT '1e-50'::float4",
    "SELECT '1.25'::real+'2.5'::real,'NaN'::float8/0::float8,'Infinity'::float8-'Infinity'::float8",
    "SELECT 'NaN'::float8='NaN'::float8,'NaN'::float8>'Infinity'::float8",
    "SELECT 1::float8/0::float8",
    "CREATE TABLE dc_float(id int PRIMARY KEY,f real,g double precision)",
    "INSERT INTO dc_float VALUES(1,'1.25','2.50'),(2,'-0','Infinity'),(3,'NaN','-Infinity') RETURNING *",
    "SELECT id,f,g FROM dc_float ORDER BY id",
    "SELECT '1.23456789'::real::numeric,'1.2345678901234567'::float8::numeric,'NaN'::float8::numeric",
    "SELECT DISTINCT x FROM (SELECT 'NaN'::float8 AS x UNION ALL SELECT 'NaN'::float8 UNION ALL SELECT '-0'::float8 UNION ALL SELECT '0'::float8) d ORDER BY x",
    "SELECT '1'::float4+16777217,'1'::float4+1.125,'16777216'::float4=16777217",
    "SELECT coalesce('1'::float4,2::float8),x FROM (SELECT '1'::float4 AS x UNION ALL SELECT 2::float8) d ORDER BY x",
    "SELECT min(f),max(f),sum(f),avg(f),min(g),max(g),sum(g),avg(g) FROM dc_float",
    "SET extra_float_digits=0",
    "SELECT '1.23456789'::real,'1.2345678901234567'::float8,'1e20'::float8",
    "SET extra_float_digits=-3",
    "SELECT '1.23456789'::real,'1.2345678901234567'::float8",
    "RESET extra_float_digits",
    "SELECT '1e-45'::float4,'1e-320'::float8,'-0e-10000'::float8",
    "SELECT '0x1.8p+1'::float8",
    "SELECT 'NaN'::float8%1::float8 WHERE false",
    "SELECT 1::float,1::float(24),1::float(25),1::float(53)",
    "SELECT 1::float(0)",
    "SELECT 1::float(54)",
    "SELECT '0e-999999'::float8,'-0e999999'::float4",
    "SELECT '1e-999999'::float8",
    "SELECT x FROM (SELECT 1::int AS x) a JOIN (SELECT 1::float4 AS x) b USING (x)",
    "SELECT x FROM (SELECT 1::float4 AS x) a FULL JOIN (SELECT 1::float8 AS x) b USING (x)",
    "SELECT 1::real+'2',1::real+NULL",
    "SELECT '2000-01-01'::date,'1970-01-01'::date,'0001-01-01 BC'::date,'infinity'::date",
    "SELECT '01/02/2003'::date,'03/04/05'::date,'01/02/69'::date,'01/02/70'::date",
    "SELECT '19990108'::date,'990108'::date,'Jan 8 1999'::date,'08-Jan-1999'::date,'1999-Jan-08'::date",
    "SELECT '2000-060'::date,'2001.366'::date,'2000-060 04:05:06'::timestamp",
    "SELECT 'JanXYZ 1 2024'::date",
    "SELECT '-2024-01-01'::date",
    "SELECT '2024-01-01-'::date",
    "SELECT 'Jan 8 1999 04:05:06'::timestamp,'01/02/2003 04:05:06+02'::timestamptz",
    "SELECT '2024-01-01 12:00:00 AD'::timestamp,'2024-01-01 12:00:00+00 AD'::timestamptz",
    "SET DateStyle TO 'ISO, DMY'",
    "CREATE TABLE dc_date_defaults(id int PRIMARY KEY,d date DEFAULT '01/02/2024' CHECK (d >= '01/02/2024'))",
    "CREATE TABLE dc_date_complex(d date CHECK(d >= DATE '2024-01-01' AND array_length(ARRAY['a'],1)>0))",
    "CREATE TABLE dc_date_empty_array(d date CHECK(d >= DATE '2024-01-01' AND array_length(ARRAY[]::text[],1) IS NULL))",
    "CREATE TABLE dc_date_node(d date CHECK(d >= DATE '2024-01-01' AND pg_get_expr(NULL,0) IS NULL))",
    "CREATE TABLE dc_date_any(d date, i int, CHECK(d >= DATE '2024-01-01' AND i=ANY('{1,2}')), CHECK(DATE '2024-01-01'=ANY(NULL)))",
    "SELECT '01/02/2003'::date,'03/04/05'::date,'2003-01-02'::date",
    "SELECT '08/01/1999 04:05:06'::timestamp,'08-Jan-1999 04:05:06'::timestamp",
    "SET DateStyle TO 'ISO'",
    "SELECT current_setting('DateStyle')",
    "SET DateStyle TO 'YMD'",
    "SELECT '03/04/05'::date,'03-Jan-04'::date",
    "SET DateStyle TO 'ISO, DEFAULT'",
    "SELECT current_setting('DateStyle')",
    "SET DateStyle TO 'DEFAULT, DMY'",
    "SELECT current_setting('DateStyle')",
    "SET DateStyle TO 'DMY, DEFAULT'",
    "SELECT current_setting('DateStyle')",
    "SET DateStyle TO 'ISO, MDY'",
    "INSERT INTO dc_date_defaults(id) VALUES(1) RETURNING *",
    "INSERT INTO dc_date_defaults VALUES(2,DATE '2024-01-31')",
    "SET TimeZone TO 'Europe/Warsaw'",
    "SET DateStyle TO 'SQL, DMY'",
    "SELECT DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.123', TIMESTAMPTZ '2024-01-02 03:04:05+00'",
    "SELECT TIMESTAMPTZ '2050-07-02 03:04:05+00', TIME '12:34:56', '12:34:56+02'::timetz",
    "SELECT (TIMESTAMPTZ '2024-01-02 03:04:05+00'::text)::timestamptz, (TIMESTAMPTZ '2024-07-02 03:04:05+00'::text)::timestamptz",
    "SET DateStyle TO 'SQL, YMD'",
    "SELECT DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05'",
    "SET DateStyle TO 'German'",
    "SELECT current_setting('DateStyle'), DATE '2024-01-02', TIMESTAMPTZ '2024-01-02 03:04:05+00'",
    "SET DateStyle TO 'Postgres, DMY'",
    "SELECT DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMPTZ '2024-07-02 03:04:05+00'",
    "SELECT TIMESTAMP '0001-01-01 01:02:03 BC', TIMESTAMP '10000-01-01 01:02:03'",
    "SELECT (TIMESTAMP '2024-01-02 03:04:05'::text)::timestamp, (TIMESTAMPTZ '2024-07-02 03:04:05+00'::text)::timestamptz",
    "SET DateStyle TO 'Postgres, MDY'",
    "SELECT DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05'",
    "SET DateStyle TO 'ISO, MDY'",
    "SET TimeZone TO 'UTC'",
    "SELECT '2024-07-01 12:00 EST'::timestamptz, '2024-07-01 12:00 EDT'::timestamptz, '2024-07-01 12:00 CST'::timestamptz, '2024-07-01 12:00 IST'::timestamptz",
    "SELECT '12:34:56 EST'::timetz, '12:34:56 EDT'::timetz, '12:34:56 CST'::timetz, '12:34:56 IST'::timetz",
    "SELECT '12:00 MEZ'::timetz, '12:00 MESZ'::timetz, '12:00 Z'::timetz",
    "SELECT 'allballs CET'::timetz",
    "SELECT '2024-01-01 12:00+02 CET'::timestamptz",
    "SELECT '2024-01-01 12:00Z CET'::timestamptz",
    "SELECT '1900-01-01 12:00 MSK'::timestamptz, '2010-01-01 12:00 MSK'::timestamptz, '2024-01-01 12:00 MSK'::timestamptz",
    "SET TimeZone TO 'Asia/Shanghai'",
    "SELECT '2024-07-01 12:00 CST'::timestamptz",
    "SET TimeZone TO 'America/New_York'",
    "SELECT '2024-11-03 01:30 EST'::timestamptz, '2024-11-03 01:30 EDT'::timestamptz, '2024-07-01 12:00 EST'::timestamptz",
    "SET TimeZone TO 'UTC'",
    "SELECT '2024-02-29'::date,'1900-02-29'::date",
    "CREATE TABLE dc_dates(id integer PRIMARY KEY,d date UNIQUE)",
    "INSERT INTO dc_dates VALUES(1,'2024-02-29'),(2,'0001-01-01 BC'),(3,'infinity') RETURNING *",
    "SELECT d FROM dc_dates ORDER BY d",
    "SELECT min(d),max(d) FROM dc_dates",
    "SELECT '2000-01-01'::date+1,'2000-01-01'::date-1,'2000-01-02'::date-'2000-01-01'::date",
    "SELECT 'infinity'::date+1,'infinity'::date-'2000-01-01'::date",
    "SELECT DATE '2000-02-29'+1,DATE '2000-03-01'-DATE '2000-02-29'",
    "SELECT '23:59:59.999999'::time,'24:00:00'::time,'12:34'::time,'12:34:56.555'::time(2)",
    "SELECT '24:00:01'::time",
    "SELECT '12:00:00+02'::timetz,'10:00:00+00'::time with time zone",
    "SELECT '12:00:00'::timetz",
    "SELECT '12:00:00+123'::timetz,'2000-01-01 12:00:00+123'::timestamptz",
    "SELECT '12:00:00+123456'::timetz",
    "SELECT '12:00:00+02'::timetz='10:00:00+00'::timetz,'12:00:00+02'::timetz<'10:00:00+00'::timetz",
    "CREATE TABLE dc_timetz(id integer PRIMARY KEY,t timetz UNIQUE)",
    "INSERT INTO dc_timetz VALUES (1,'12:00:00+02'),(2,'10:00:00+00'),(3,'09:00:00-01') RETURNING *",
    "SELECT id,t FROM dc_timetz ORDER BY t",
    "SELECT '00:00:60.5'::time,'24:00:00.0000001'::time",
    "SELECT '00:00:00.0001255'::time,'00:00:00.0001265'::time",
    "SELECT '12:34:56.'::time",
    "CREATE TABLE dc_times(id int PRIMARY KEY,t time(2) UNIQUE)",
    "INSERT INTO dc_times VALUES(1,'12:34:56.555'),(2,'24:00:00'),(3,'00:00:00') RETURNING *",
    "SELECT t FROM dc_times ORDER BY t",
    "SELECT TIME '12:34:56.5'",
    "SELECT '2000-01-01 12:34:56.123456'::timestamp,'2000-01-01 12:34:56+02'::timestamptz",
    "SELECT '1999-12-31 23:59:59.75'::timestamp(0),'infinity'::timestamp",
    "CREATE TABLE dc_stamps(id int PRIMARY KEY, t timestamp(2),z timestamptz)",
    "INSERT INTO dc_stamps VALUES(1,'1999-12-31 23:59:59.125','2000-01-01 12:34:56+02'),(2,'infinity','-infinity') RETURNING *",
    "SELECT id,t,z FROM dc_stamps ORDER BY id",
    "SELECT TIMESTAMP '2000-01-01 00:00:00'",
    "SELECT '4714-11-23 23:00:00-01 BC'::timestamptz",
    "SELECT '4714-11-24 00:00:59.999999+00:01 BC'::timestamptz(0)",
    "SELECT '2000-01-01T00:00:00 +02'::timestamptz",
    "SELECT '2000-01-01 12:00:00+0230'::timestamptz",
    "SELECT '2024-07-15 12:00:00 Europe/Warsaw'::timestamptz,'2024-01-15 12:00:00 America/New_York'::timestamptz",
    "SELECT '2024-01-15 12:00:00 america/new_york'::timestamptz",
    "SELECT '2050-03-27 02:30:00 Europe/Warsaw'::timestamptz,'2050-10-30 02:30:00 Europe/Warsaw'::timestamptz",
    "SET TIME ZONE 'Europe/Warsaw'",
    "SET TIME ZONE 'europe/warsaw'",
    "SELECT '12:00:00'::timetz",
    "SELECT '2024-01-15 12:00:00'::timestamptz,'2024-07-15 12:00:00'::timestamptz",
    "SELECT 'epoch'::timestamp,'epoch'::timestamptz",
    "SELECT 'today'::date='now'::date,'tomorrow'::date-'today'::date,'today'::date-'yesterday'::date",
    "SELECT 'today 12:34:56'::timestamp::date='today'::date,'today 12:34:56'::timestamp::time=TIME '12:34:56'",
    "SELECT 'now'::timestamp='now'::timestamp,'now'::timestamptz='now'::timestamptz,'now'::time='now'::time,'now'::timetz='now'::timetz",
    "SELECT 'now+02'::timetz IS NOT NULL",
    "SELECT 'nowZ'::timetz IS NOT NULL",
    "SELECT 'allballs'::time,'allballs'::timetz,'2000-01-01 allballs'::timestamptz",
    "SELECT 'allballs +02'::timetz",
    "SELECT 'allballs Z'::timetz",
    "SELECT 'today'::time",
    "SELECT 'allballs'::timestamp",
    "SELECT '2024-03-31 02:30:00'::timestamptz,'2024-10-27 02:30:00'::timestamptz",
    "SELECT '2024-03-30 12:00:00'::timestamptz+INTERVAL '1 day','2024-03-30 12:00:00'::timestamptz+INTERVAL '24 hours'",
    "SELECT '2024-10-26 12:00:00'::timestamptz+INTERVAL '1 day','2024-10-28 12:00:00'::timestamptz-INTERVAL '1 day'",
    "SELECT '2024-02-29 02:30:00'::timestamptz+INTERVAL '1 month 1 day',INTERVAL '1 day'+'2024-03-30 12:00:00'::timestamptz",
    "SELECT '2024-03-31 12:00:00'::timestamptz-'2024-03-30 12:00:00'::timestamptz",
    "SELECT 'infinity'::timestamptz+INTERVAL '-infinity'",
    "SELECT '2024-01-01 23:30:00+00'::timestamptz::date,'2024-07-01 23:30:00+00'::timestamptz::time",
    "SELECT '2024-01-15 12:00:00'::timestamp::timestamptz,'2024-07-15'::date::timestamptz",
    "SELECT '2050-01-15 12:00:00'::timestamptz,'2050-07-15 12:00:00'::timestamptz",
    "SELECT '2050-03-27 02:30:00'::timestamptz,'2050-10-30 02:30:00'::timestamptz",
    "SELECT '2100-01-15 12:00:00'::timestamptz,'2100-07-15 12:00:00'::timestamptz",
    "SET TIME ZONE 'Australia/Lord_Howe'",
    "SELECT '2050-01-15 12:00:00'::timestamptz,'2050-07-15 12:00:00'::timestamptz",
    "SET TIME ZONE 'Europe/Warsaw'",
    "SELECT 'infinity'::timestamptz,'-infinity'::timestamptz",
    "SET TIME ZONE 'Etc/GMT+12'",
    "SELECT TIMESTAMP '294276-12-31 23:59:59'::timestamptz",
    "SET TIME ZONE 'Etc/GMT-12'",
    "SELECT '294276-12-31 23:59:59+00'::timestamptz::timestamp",
    "SET TIME ZONE 'Etc/GMT-14'",
    "SELECT TIMESTAMPTZ '294276-12-30 23:00:00+00' + INTERVAL '1 day'",
    "SET TIME ZONE 'UTC'",
    "BEGIN",
    "SET TIME ZONE 'Europe/Warsaw'",
    "SELECT '2024-07-15 12:00:00+00'::timestamptz",
    "ROLLBACK",
    "SELECT '2024-07-15 12:00:00+00'::timestamptz",
    "SET TIME ZONE 'right/America/New_York'",
    "SELECT DATE '2000-01-01'::timestamp, TIMESTAMP '2000-01-01 12:00:00'::date, TIMESTAMP '2000-01-01 12:00:00'::time",
    "SELECT DATE '2000-01-01'=TIMESTAMP '2000-01-01 00:00:00'",
    "SELECT '1 year 2 mons 3 days 04:05:06.7'::interval,'-1 mon 2 days -03:00:00'::interval",
    "SELECT 'P1Y2M3DT4H5M6.7S'::interval,'P1.5Y'::interval,'P2W'::interval",
    "SELECT 'P00010203T040506'::interval,'P1-2-3T4:05:06.7'::interval",
    "SELECT 'PT1e2S'::interval,'PT1H1H'::interval,'PT'::interval",
    "SELECT 'P00010203T040506.5'::interval",
    "SELECT 'PT0.0000005S'::interval,'PT0.0000015S'::interval,'P0.25M'::interval,'P-1.5Y'::interval",
    "SELECT 'P1-2T04:05'::interval,'PT123456'::interval,'P2024'::interval",
    "SELECT 'P1Y2D3Y'::interval,'P1DT2DT3H'::interval",
    "SELECT 'P178956971Y-2147483647M'::interval,'P178956971Y'::interval",
    "SELECT 'P0x1Y'::interval,'PT0x1.8p1S'::interval",
    "SELECT 'PT1e-310S'::interval",
    "SELECT 'PT0x1p-1023S'::interval,'PT0x1p-1074S'::interval",
    "SELECT 'PT0x1.0000000000001p-1023S'::interval",
    f"SELECT 'PT{5 ** 1074}e-1074S'::interval",
    "SELECT 'PT-NaNS'::interval",
    "SELECT ' P1D'::interval",
    "SELECT 'P1D '::interval",
    "SELECT 'P2147483647M2147483647DT9223372036854S0.775807S'::interval",
    "SELECT INTERVAL '1' YEAR, INTERVAL '1' YEAR TO MONTH, INTERVAL '1' DAY TO HOUR",
    "SELECT '1'::interval, '1'::interval day, '1'::interval::interval day",
    "SELECT ('2'::text)::interval minute, '2'::interval minute",
    "SELECT INTERVAL '1 year 2 mons 3 days 04:05:06' DAY, INTERVAL '-1 year -11 mons' YEAR",
    "SELECT INTERVAL '-00:59:59' HOUR, INTERVAL '23:59:59.999999' SECOND(0)",
    "SELECT INTERVAL '1.25 seconds' SECOND(1), INTERVAL '-1.25 seconds' SECOND(1), INTERVAL(1) '1.25 seconds'",
    "SELECT INTERVAL 'infinity' DAY, 'infinity'::interval(1)",
    "SELECT INTERVAL '1 mon 2 days 03:04:05' * 1.5, 2 * INTERVAL '1 day', INTERVAL '1 mon' / 2",
    "SELECT INTERVAL '-1 mon -1 day -03:00:00' * 0.5, INTERVAL '1 mon 2 days' / 3.0",
    "SELECT INTERVAL '1 day' * 'Infinity'::float8, INTERVAL '1 day' / 'Infinity'::float8",
    "SELECT INTERVAL 'infinity' * -2, INTERVAL '-infinity' / -2",
    "SELECT INTERVAL '1 day' / 0",
    "SELECT INTERVAL '0' * 'Infinity'::float8",
    "SELECT INTERVAL 'infinity' * 0",
    "SELECT INTERVAL 'infinity' / 'Infinity'::float8",
    "SELECT INTERVAL '1 day' * 'NaN'::float8",
    "SELECT INTERVAL '1 microsecond' * 0.5, INTERVAL '3 microseconds' / 2, INTERVAL '24 hours' * 2",
    "SELECT INTERVAL '5849 months 1570 days' * 65.59506384011272",
    "SELECT INTERVAL '1.0000015 seconds', INTERVAL '-1.0000015 seconds', INTERVAL '1.5 milliseconds', INTERVAL '2 microseconds'",
    "SELECT INTERVAL '1 mon -30 days' * 'Infinity'::float8",
    "CREATE TABLE dc_interval_agg(id int PRIMARY KEY, i interval)",
    "INSERT INTO dc_interval_agg VALUES (1,INTERVAL '1 mon'),(2,INTERVAL '0'),(3,NULL)",
    "SELECT sum(i),avg(i) FROM dc_interval_agg",
    "SELECT sum(i),avg(i) FROM dc_interval_agg WHERE id>2",
    "SELECT id,sum(i) OVER (ORDER BY id),avg(i) OVER (ORDER BY id) FROM dc_interval_agg ORDER BY id",
    "INSERT INTO dc_interval_agg VALUES (4,INTERVAL 'infinity'),(5,INTERVAL '-infinity')",
    "SELECT sum(i),avg(i) FROM dc_interval_agg WHERE id<5",
    "SELECT sum(i) FROM dc_interval_agg",
    "CREATE TABLE dc_interval_overflow(id int PRIMARY KEY, i interval)",
    "INSERT INTO dc_interval_overflow VALUES (1,INTERVAL 'infinity'),(2,INTERVAL '2147483647 months'),(3,INTERVAL '1 month'),(4,INTERVAL '-1 month')",
    "SELECT sum(i) FROM dc_interval_overflow",
    "SELECT avg(i) FROM dc_interval_overflow",
    "SELECT INTERVAL '12:34' HOUR TO MINUTE, INTERVAL '12:34' MINUTE TO SECOND, INTERVAL '12:34.5'",
    "SELECT INTERVAL '1 02:03' DAY TO MINUTE, INTERVAL '-1 -02:03:04' DAY TO SECOND",
    "SELECT INTERVAL '1-2' YEAR TO MONTH, INTERVAL '-1-2' YEAR TO MONTH, INTERVAL '1-2 3 04:05:06'",
    "SELECT INTERVAL '2 days 1-2' YEAR TO MONTH",
    "SELECT INTERVAL '1-12' YEAR TO MONTH",
    "SELECT INTERVAL '1' YEAR TO YEAR",
    "SELECT '1 year 2 mons'::\"interval\"(4), '1.25 seconds'::pg_catalog.interval(32767,1)",
    "SELECT '1'::\"interval\"(1000)",
    "CREATE TABLE dc_interval_mods(id int PRIMARY KEY, a interval year to month, b interval day to second(2), c interval(0))",
    "INSERT INTO dc_interval_mods VALUES (1,'1 year 3 mons 7 days','1 day 02:03:04.125','-1.5 seconds') RETURNING *",
    "SELECT id,a,b,c FROM dc_interval_mods ORDER BY id",
    "CREATE TABLE dc_interval_minute(id int PRIMARY KEY, m interval minute)",
    "CREATE TABLE dc_interval_generic(id int PRIMARY KEY, i \"interval\"(4,3))",
    "INSERT INTO dc_interval_generic VALUES(1,'1 year 2 mons') RETURNING *",
    "INSERT INTO dc_interval_minute SELECT 1,'2' RETURNING *",
    "UPDATE dc_interval_minute SET m = (SELECT '3') WHERE id=1 RETURNING *",
    "SELECT '1_0 days'::interval",
    "SELECT '1e2 days'::interval",
    "SELECT '1 month'::interval='30 days'::interval,'0'::interval",
    "CREATE TABLE dc_intervals(id int PRIMARY KEY,v interval UNIQUE)",
    "INSERT INTO dc_intervals VALUES(1,'1 month'),(2,'2 days'),(3,'-03:04:05') RETURNING *",
    "INSERT INTO dc_intervals VALUES(4,'30 days')",
    "SELECT id,v FROM dc_intervals ORDER BY v",
    "SELECT TIMESTAMP '2024-01-31 12:00:00'+INTERVAL '1 month',DATE '2024-02-29'+INTERVAL '1 year'",
    "SELECT TIMESTAMP '2000-01-02 01:00:00'-TIMESTAMP '2000-01-01 00:00:00', TIME '00:00:00'-TIME '01:00:00'",
    "SELECT INTERVAL '1 month 2 days'+INTERVAL '3 days', TIME '23:30:00'+INTERVAL '2 hours'",
    "SELECT INTERVAL '0.1 year', INTERVAL '0.01 year', INTERVAL '0.0001255 seconds'",
    "SELECT INTERVAL '1 day 2 days'",
    "SELECT INTERVAL 'NaN days'",
    "SELECT INTERVAL 'infinity', INTERVAL '-infinity', -INTERVAL 'infinity'",
    "SELECT INTERVAL 'infinity'+INTERVAL '1 day'",
    "SELECT INTERVAL 'infinity'+INTERVAL '-infinity'",
    "SELECT TIMESTAMP 'infinity'-TIMESTAMP '2000-01-01'",
    "SELECT TIME '12:00:00'+INTERVAL 'infinity'",
    "SELECT TIMESTAMP '294276-12-31 12:00:00'+INTERVAL '1 day -24 hours'",
)


class NumericErrorConnection(QueryConnection):
    def receive(self):
        kind, body = super().receive()
        if kind == b"E":
            fields = self.error_fields(body)
            self.numeric_error = tuple(fields.get(key) for key in ("C", "M", "D", "H"))
        return kind, body

    def observe_numeric_error(self, sql):
        self.numeric_error = None
        self.observe(sql)
        return self.numeric_error


def check_numeric_prefix_errors(reference, native):
    peers = [NumericErrorConnection(port, "numeric-prefix-errors")
             for port in (reference, native)]
    try:
        for operator in ("@", "|/", "||/", "~"):
            for operand, type_name in (("true", "boolean"), ("'x'::text", "text"),
                                       ("NULL::text", "text"), ("ARRAY[1]", "integer[]"),
                                       ("'1'::oid", "oid")):
                sql = f"SELECT {operator} {operand}"
                expected = ("42883", f"operator does not exist: {operator} {type_name}",
                            "No operator of that name accepts the given argument type.",
                            "You might need to add an explicit type cast.")
                outcomes = [peer.observe_numeric_error(sql) for peer in peers]
                assert outcomes == [expected, expected], (sql, outcomes)
            for operand, state in (("missing_prefix_operand", "42703"),
                                   ("'bad'::integer", "22P02"),
                                   ("'bad'", "42725" if operator == "~" else "22P02")):
                sql = f"SELECT {operator} {operand}"
                outcomes = [peer.observe_numeric_error(sql) for peer in peers]
                assert all(error is not None and error[0] == state for error in outcomes), (sql, outcomes)
        for operator in ("&", "|", "#", "<<", ">>", "^"):
            for left, right, signature in (("true", "1", f"boolean {operator} integer"),
                                           ("'1'", "true", f"unknown {operator} boolean"),
                                           ("'x'::text", "1", f"text {operator} integer")):
                sql = f"SELECT {left} {operator} {right}"
                expected = ("42883", f"operator does not exist: {signature}",
                            "No operator of that name accepts the given argument types.",
                            "You might need to add explicit type casts.")
                outcomes = [peer.observe_numeric_error(sql) for peer in peers]
                assert outcomes == [expected, expected], (sql, outcomes)
        for left, right, signature in (
                ("NULL::text", "NULL", "text ^ unknown"),
                ("NULL", "NULL::boolean", "unknown ^ boolean"),
                ("true", "'bad'", "boolean ^ unknown"),
                ("1::oid", "2", "oid ^ integer"),
                ("1", "ARRAY[2]", "integer ^ integer[]")):
            sql = f"SELECT {left} ^ {right}"
            expected = ("42883", f"operator does not exist: {signature}",
                        "No operator of that name accepts the given argument types.",
                        "You might need to add explicit type casts.")
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        for name in ("trunc", "pg_catalog.trunc"):
            for argument in ("'2'", "'bad'", "NULL"):
                sql = f"SELECT {name}({argument})"
                expected = ("42725", f"function {name}(unknown) is not unique",
                            "Could not choose a best candidate function.",
                            "You might need to add explicit type casts.")
                outcomes = [peer.observe_numeric_error(sql) for peer in peers]
                assert outcomes == [expected, expected], (sql, outcomes)
        for sql, state, message in (
                ("SELECT missing_power_operand ^ true", "42703",
                 'column "missing_power_operand" does not exist'),
                ("SELECT 'bad'::numeric ^ true", "22P02",
                 'invalid input syntax for type numeric: "bad"'),
                ("SELECT 'bad' ^ '2'", "22P02",
                 'invalid input syntax for type double precision: "bad"'),
                ("SELECT 0 ^ (-1)", "2201F",
                 "zero raised to a negative power is undefined")):
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [(state, message, None, None)] * 2, (sql, outcomes)
        ambiguous = [("SELECT ~ '1'", "~ unknown"), ("SELECT ~ NULL", "~ unknown")]
        ambiguous.extend((f"SELECT '1' {operator} '2'", f"unknown {operator} unknown")
                         for operator in ("&", "|", "#", "<<", ">>"))
        ambiguous.extend((f"SELECT '1' {operator} 1::smallint", f"unknown {operator} smallint")
                         for operator in ("<<", ">>"))
        for sql, signature in ambiguous:
            expected = ("42725", f"operator is not unique: {signature}",
                        "Could not choose a best candidate operator.",
                        "You might need to add explicit type casts.")
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        for operator in ("<<", ">>"):
            sql = f"SELECT '1' {operator} 1::bigint"
            expected = ("42883", f"operator does not exist: unknown {operator} bigint",
                        "No operator of that name accepts the given argument types.",
                        "You might need to add explicit type casts.")
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        for sql in ("SELECT @ NULL,|/ NULL,||/ NULL",
                    "SELECT @ (-4)::numeric,|/ 4::numeric,||/ 8::numeric",
                    "SELECT |/ (-1)::numeric", "SELECT '1' << 1::integer,'1' >> 1::integer",
                    "SELECT 5 % '2','5' % 2::int2,5::int8 % '2','5.5' % 2::numeric,NULL % 2",
                    "SELECT '-9223372036854775808'::int8 % '-1'"):
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        for sql, signature, state in (
                ("SELECT '3' % '2'", "unknown % unknown", "42725"),
                ("SELECT NULL % NULL", "unknown % unknown", "42725"),
                ("SELECT NULL::boolean % 2", "boolean % integer", "42883"),
                ("SELECT 2 % NULL::text", "integer % text", "42883"),
                ("SELECT 2::float8 % 1", "double precision % integer", "42883")):
            ambiguous = state == "42725"
            expected = (state,
                        f"operator {'is not unique' if ambiguous else 'does not exist'}: {signature}",
                        "Could not choose a best candidate operator." if ambiguous else
                        "No operator of that name accepts the given argument types.",
                        "You might need to add explicit type casts.")
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        for sql, signature, wrong_count in (
                ("SELECT int4mod('x',true)", "int4mod(unknown, boolean)", False),
                ("SELECT pg_catalog.int4mod('x',true)", "pg_catalog.int4mod(unknown, boolean)", False),
                ("SELECT int2mod(7,2)", "int2mod(integer, integer)", False),
                ("SELECT int8and('x',true)", "int8and(unknown, boolean)", False),
                ("SELECT int4mod()", "int4mod()", True)):
            expected = ("42883", f"function {signature} does not exist",
                        "No function of that name accepts the given number of arguments." if wrong_count else
                        "No function of that name accepts the given argument types.",
                        None if wrong_count else "You might need to add explicit type casts.")
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        print("numeric-operators: exact SQLSTATE/message/detail/hint and coercion order PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check_smallint_casts(reference, native):
    peers = [NumericErrorConnection(port, "smallint-casts") for port in (reference, native)]
    try:
        for sql in ("SELECT 32767::bigint::smallint, (-32768)::bigint::smallint",
                    "SELECT 32766.5::float8::smallint, (-32768.5)::float4::smallint",
                    "SELECT 32766.5::numeric::smallint, (-32768.4)::numeric::smallint"):
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        for sql, state, message in (
                ("SELECT 9223372036854775807::bigint::smallint", "22003", "smallint out of range"),
                ("SELECT (-9223372036854775808)::bigint::smallint", "22003", "smallint out of range"),
                ("SELECT 1e30::numeric::smallint", "22003", "smallint out of range"),
                ("SELECT 32767.5::numeric::smallint", "22003", "smallint out of range"),
                ("SELECT (-32768.5)::numeric::smallint", "22003", "smallint out of range"),
                ("SELECT 32767.5::float8::smallint", "22003", "smallint out of range"),
                ("SELECT (-32769)::float4::smallint", "22003", "smallint out of range"),
                ("SELECT 1e30::float8::smallint", "22003", "smallint out of range"),
                ("SELECT 1e30::float4::smallint", "22003", "smallint out of range"),
                ("SELECT 'NaN'::numeric::smallint", "0A000", "cannot convert NaN to smallint"),
                ("SELECT '-Infinity'::numeric::smallint", "0A000", "cannot convert infinity to smallint"),
                ("SELECT 'NaN'::float8::smallint", "22003", "smallint out of range"),
                ("SELECT 'Infinity'::float4::smallint", "22003", "smallint out of range")):
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [(state, message, None, None)] * 2, (sql, outcomes)
        print("smallint-casts: values and exact errors PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check_integer_input_errors(reference, native):
    peers = [NumericErrorConnection(port, "integer-input-errors")
             for port in (reference, native)]
    try:
        for type_name, sql_type, minimum, maximum in (
                ("smallint", "int2", -(2 ** 15), 2 ** 15 - 1),
                ("integer", "int4", -(2 ** 31), 2 ** 31 - 1),
                ("bigint", "int8", -(2 ** 63), 2 ** 63 - 1)):
            overflow = str(maximum + 1)
            syntax_inputs = ("bad", "  bad  ", 'a"b\\c', "bad\nvalue", "", "1_",
                             overflow + "x")
            range_inputs = (overflow, str(minimum - 1), f"  {overflow}  ",
                            overflow + "0x")
            for input_text in syntax_inputs + range_inputs:
                expected = (("22P02", f'invalid input syntax for type {type_name}: "{input_text}"',
                             None, None) if input_text in syntax_inputs else
                            ("22003", f'value "{input_text}" is out of range for type {type_name}',
                             None, None))
                for sql in (f"SELECT '{input_text}'::{sql_type}",
                            f"SELECT {sql_type}and('{input_text}', 1::int2)",
                            f"SELECT 1::{sql_type} & '{input_text}'"):
                    outcomes = [peer.observe_numeric_error(sql) for peer in peers]
                    assert outcomes == [expected, expected], (sql, outcomes)
            sql = f"SELECT '{minimum}'::{sql_type}, '{maximum}'::{sql_type}"
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        for sql in ("SELECT 1::int2 << 'bad'", "SELECT int8shr(1, 'bad')"):
            expected = ("22P02", 'invalid input syntax for type integer: "bad"', None, None)
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        print("integer-input: exact SQLSTATE/message/detail/hint and error order PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check_numeric_input_errors(reference, native):
    peers = [NumericErrorConnection(port, "numeric-input-errors")
             for port in (reference, native)]
    try:
        overflow_inputs = ("1e131072", "1e-16384", "0e-16384", "-1e-16384",
                           "1e1073741824", "0e1073741824", "1e-1073741824",
                           "1e1073741824junk", "1e1073741824_")
        syntax_inputs = ("1e131072junk", "1e-16384junk", "0e-16384junk",
                         "1e1073741823junk", "1e131072_")
        for input_text in overflow_inputs + syntax_inputs:
            sql = f"SELECT '{input_text}'::numeric"
            expected = (("22003", "value overflows numeric format", None, None)
                        if input_text in overflow_inputs else
                        ("22P02", f'invalid input syntax for type numeric: "{input_text}"',
                         None, None))
            outcomes = [peer.observe_numeric_error(sql) for peer in peers]
            assert outcomes == [expected, expected], (sql, outcomes)
        sql = "SELECT '0e1073741823'::numeric::text, scale('1e-16383'::numeric)::text"
        outcomes = [peer.observe(sql) for peer in peers]
        assert outcomes[0] == outcomes[1], (sql, outcomes)
        print("numeric-input: exact SQLSTATE/message/detail/hint and error order PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check(reference, native):
    peers = [QueryConnection(port, "defaults-checks-aggregates") for port in (reference, native)]
    failures = []
    try:
        for peer in peers:
            peer.observe("SET TIME ZONE 'UTC'")
            for sql in CASES:
                if sql.startswith("CREATE TABLE "):
                    peer.observe("DROP TABLE IF EXISTS " + sql.split("(")[0].split()[2])
            for name in ("dc_seq", "dc_order_seq"):
                peer.observe("DROP SEQUENCE IF EXISTS " + name)
        for index, sql in enumerate(CASES, 1):
            outcomes = [peer.observe(sql) for peer in peers]
            if outcomes[0] != outcomes[1]:
                failures.append((index, sql, outcomes))
                print("defaults-checks-aggregates: FAIL", failures[-1], flush=True)
        assert not failures, failures
        print(f"defaults-checks-aggregates: {len(CASES)} C comparisons PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


def check_datestyle_sessions(reference, native):
    barrier = Barrier(4)

    def worker(port, order):
        peer = QueryConnection(port, "datestyle-isolation")
        try:
            peer.observe("SET DateStyle TO 'ISO, " + order + "'")
            barrier.wait(timeout=10)
            return [peer.observe("SELECT '01/02/2003'::date") for _ in range(10)]
        finally:
            peer.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [pool.submit(worker, port, order)
                 for port in (reference, native) for order in ("MDY", "DMY")]
        results = [task.result() for task in tasks]
    assert results[0] == results[2] and results[1] == results[3], results
    assert results[0] != results[1], results
    print("datestyle: concurrent session isolation PASS", flush=True)


def check_transaction_time(reference, native):
    def worker(port):
        peer = QueryConnection(port, "transaction-time")
        try:
            peer.observe("SET TimeZone TO 'UTC'")
            peer.observe("BEGIN")
            sql = "SELECT 'now'::date,'now'::time,'now'::timetz,'now'::timestamp,'now'::timestamptz"
            first = peer.observe(sql)
            time.sleep(1.1)
            peer.observe("SET TimeZone TO 'Europe/Warsaw'")
            peer.observe("SET TimeZone TO 'UTC'")
            second = peer.observe(sql)
            assert first == second, (first, second)
            peer.observe("ROLLBACK")
        finally:
            peer.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [pool.submit(worker, port) for port in (reference, native)]
        for task in tasks:
            task.result()
    print("datetime: transaction-start stability PASS", flush=True)


def check_restart(reference, native):
    peers = [QueryConnection(port, "default-check-restart") for port in (reference, native)]
    try:
        for peer in peers:
            peer.observe("SET TIME ZONE 'UTC'")
        for sql in ("INSERT INTO dc_decimal DEFAULT VALUES RETURNING a",
                    "INSERT INTO dc_decimal VALUES(0)",
                    "INSERT INTO dc_decimal VALUES(5)",
                    "ALTER TABLE dc_decimal VALIDATE CONSTRAINT decimal_later",
                    "SELECT avg(a) FROM dc_decimal",
                     "INSERT INTO dc_rows(id) VALUES(9) RETURNING *",
                     "SELECT id,n FROM dc_numeric ORDER BY id",
                     "SELECT n,count(*) FROM dc_numeric_raw GROUP BY n ORDER BY n",
                     "SELECT n FROM dc_numeric_special ORDER BY n",
                     "SELECT n FROM dc_smallint ORDER BY n"):
            outcomes = [peer.observe(sql) for peer in peers]
            assert outcomes[0] == outcomes[1], (sql, outcomes)
        print("defaults-checks: fresh-image defaults/decimal AST/CHECK/NOT VALID/sequence state PASS", flush=True)
    finally:
        for peer in peers:
            peer.close()


if __name__ == "__main__":
    if "--restart" in sys.argv:
        check_restart(int(sys.argv[1]), int(sys.argv[2]))
    else:
        check(int(sys.argv[1]), int(sys.argv[2]))
        check_numeric_prefix_errors(int(sys.argv[1]), int(sys.argv[2]))
        check_smallint_casts(int(sys.argv[1]), int(sys.argv[2]))
        check_integer_input_errors(int(sys.argv[1]), int(sys.argv[2]))
        check_numeric_input_errors(int(sys.argv[1]), int(sys.argv[2]))
        check_datestyle_sessions(int(sys.argv[1]), int(sys.argv[2]))
        check_transaction_time(int(sys.argv[1]), int(sys.argv[2]))
        if len(sys.argv) >= 5:
            from default_check_concurrency import check as concurrency
            from basic_aggregate_resources import check as resources
            concurrency(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
            resources(int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
