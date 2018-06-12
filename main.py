import psycopg2
import sys
import myfun
import os
import xlrd

class pg_pro():
    path = '/home/sxg/code/Python/data'
    dataname = '0312.xlsx'

    def __init__(self):
        print('Begin...')
        
        try:
            self.conn = psycopg2.connect("dbname='pg0605' user='postgres' host='localhost' password='523'")
        except:
            print('I am unable to connect to the database')
        self.cur = self.conn.cursor()
    
    def Creat_Tab(self,tablename):
        #self.cur.execute('drop table'+tablename)
        #self.cur.execute('create table'+tablename+'''
        #        (
        #            city            varchar(80),
        #            temp_lo         int,           -- low temperature
        #            temp_hi         int,           -- high temperature
        #            prcp            real,          -- precipitation
        #            date            date
        #        );
        #        ''')
        self.cur.execute('create table'+tablename+'''
        (GPSTime DOUBLE precision PRIMARY KEY      NOT NULL,
         Latitude            double precision      NOT NULL,
         Longitude           double precision     NOT NULL,
         Alt               double precision     NOT NULL);
        ''')


    def data_pro(self,tablename):
        workbook = xlrd.open_workbook(os.path.join(self.path,self.dataname))
        sheet = workbook.sheet_by_name('0312')
        nrows = sheet.nrows
        print('nums:', nrows)
        self.cur.execute('delete from'+tablename)
        for i in range(1,nrows):
            # data_list[i].append(sheet.cell(i,j).value
            # temp_lat = float(sheet.cell(i,0).value)
            # temp_lon = float(sheet.cell(i,1).value)
            # temp_alt = float(sheet.cell(i,6).value)
            # temp_gpstime = float(sheet.cell(i,3).value)
            temp_lat = sheet.cell(i,0).value
            temp_lon = sheet.cell(i,1).value
            temp_alt = sheet.cell(i,6).value
            temp_gpstime = sheet.cell(i,3).value
            # print(type(temp_gpstime))
            self.cur.execute("INSERT INTO"+ tablename + "(GPSTime,Latitude,Longitude,Alt) VALUES (%f,%f,%f,%f)"%(temp_gpstime,temp_lat,temp_lon,temp_alt))
        # self.cur.execute("INSERT INTO"+ tablename + "(ID,NAME,AGE,ADDRESS,SALARY) VALUES (2, 'Allen', 25, 'Texas', 15000.00)")
        # self.cur.execute("INSERT INTO"+ tablename + "(ID,NAME,AGE,ADDRESS,SALARY) VALUES (3, 'Teddy', 23, 'Norway', 20000.00)")
        # self.cur.execute("INSERT INTO"+ tablename + "(ID,NAME,AGE,ADDRESS,SALARY) VALUES (5, 'Mark', 25, 'Rich-Mond ', 65000.00)")

    def commit(self):
        self.conn.commit()
        self.conn.close()



def main():
    ex = pg_pro()
    tabname = ' excel0312 '
    # ex.Creat_Tab(tabname)
    ex.data_pro(tabname)
    ex.commit()

main()
