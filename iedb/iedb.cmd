echo off;
set LIB_PATH=D:\hwj\workspace\IEDB\iedb\lib
set CLASS_PATH=.;%LIB_PATH%;%LIB_PATH%\commons-logging-1.1.jar;%LIB_PATH%\IEDB.jar;%LIB_PATH%\jdom-1.0b8.jar;%LIB_PATH%\log4j-1.2.9.jar;%LIB_PATH%\ojdbc14.jar;
cmd.exe /k D:\hwj\programfiles\java\jdk1.5.0_11\bin\java -cp %CLASS_PATH% com.founder.enp.dayoo.IEDBMain hzw.xml