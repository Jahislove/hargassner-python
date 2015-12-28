#!/usr/bin/python
# -*- coding: utf-8 -*-

# auteur : Jahislove
# version 0.1 beta with partial error handling

# ce script tourne sur un Rasberry pi
# il ecoute une chaudiere a granulés Hargassner NanoPK sur son port telnet
# et il ecrit les valeurs dans une BDD MySQL ou MariaDB sur un NAS Synology
# fonctionne avec les chaudieres nanoPK, classic and HSV  equipées de touchtronic + passerelle internet
# il est possible que ca fontionne sans passerelle (a tester)

# this script is running on raspberry pi
# it listen an Hargassner NanoPK Boiler on telnet
# and then it write data in MySQL or MariaDB on a NAS Synology
# work with nanoPK, classic and HSV boiler equiped with touchtronic + internet gateway
# may work without gateway (to be tested)

# Import socket module
import socket               
import time
import datetime
import MySQLdb   # MySQLdb must be installed by yourself
import sys
#import sqlite3

PATH_HARG = "/home/pi/hargassner/" #path to this script
IP_CHAUDIERE = '192.168.0.198'
PORT = 23    
MSGBUFSIZE=1024

DB_SERVER ='192.168.0.111'  # MySQL : IP server (localhost if mySQL is on the same machine)
DB_BASE='Hargassner'        # MySQL : database name
DB_USER='hargassner'        # MySQL : user  
DB_PWD='*******'            # MySQL : password 

INSERT_GROUPED = 2         # regroupe n reception avant d'ecrire en base :INSERT_GROUPED x FREQUENCY = temps en sec
FREQUENCY = 5               # Periodicité (reduit le volume de data mais reduit la précision)
                            # (1 = toutes)     1 mesure chaque seconde
                            # (5)              1 mesure toutes les 5 secondes
                            # ...
                            # une frequence trop élevée entraine de gros volume en BDD et surtout des grosses 
                            # lenteurs pour afficher les graphiques
#----------------------------------------------------------#
#        socket for Connection to Hargassner               #
#----------------------------------------------------------#
while True:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        s.connect((IP_CHAUDIERE, PORT))
        break
    except:
        print "connexion impossible"
        time.sleep(0.5)
        
#----------------------------------------------------------#
#     definition : database query with error handling      #
#----------------------------------------------------------#

def query_db(sql):
    try:
        db = MySQLdb.connect(DB_SERVER, DB_USER, DB_PWD, DB_BASE)
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        db.close()
    except MySQLdb.Error:
        logfile = open(PATH_HARG + "hargassner.log", "a")
        log = time.strftime('%Y-%m-%d %H:%M:%S') + " ERR : MySQL is down :" + MySQLdb.Error
        logfile.write(log)
        logfile.close
    
#----------------------------------------------------------#
#             code                                         #
#----------------------------------------------------------#
# receive data from the server
i=0
tableau = []
frequencyCount = FREQUENCY  
while True:
    try:
        buffer = s.recv(MSGBUFSIZE) # waiting a packet (waiting as long as s.recv is empty), 0x40
        datebuff = time.strftime('%Y-%m-%d %H:%M:%S') #formating date for mySQL
    except KeyboardInterrupt:
        break
    try:
        if buffer[0:2] == "pm":
            frequencyCount = frequencyCount + 1
            if frequencyCount <= FREQUENCY: 
                continue     # skip code below and return to while, if we want to skip some data 
                
            frequencyCount = 1

            # insert optimisé avec boucle permettant de grouper les données avant d'ecrire en base
            liste=buffer.split()    # transforme la string du buffer en liste 
            liste[0] = datebuff     # remplace la valeur "pm" par la date
            liste = liste [0:174]   # selectionne les valeurs voulues, voir channel.txt
            i = i + 1
            liste = tuple(liste)    # transforme la liste en tuple (necessaire pour le INSERT)
            tableau.append(liste)   # cumule les tuples dans un tableau
            if i == INSERT_GROUPED:
                for x in range(INSERT_GROUPED):
                    tableau = tuple(tableau)  # crée un tuple de tuple
                    query_db("""INSERT INTO nanoPK  VALUES (null,'%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                    '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
                    """ % tableau[x] ) # null correspond a id
                i = 0
                tableau = []

        else:
			logfile = open(PATH_HARG + "trace.log", "a")
			logfile.write(buffer)
			logfile.write("----\n")
			logfile.close

    except :
        continue
        
s.close()   