#!/usr/bin/python
# -*- coding: utf-8 -*-

# auteur : Jahislove
# version 1.2
# python version 2.7

# ce script tourne sur un Rasberry pi
# il ecoute une chaudiere a granulés Hargassner NanoPK sur son port telnet
# et il ecrit les valeurs dans une BDD MySQL ou MariaDB sur un NAS Synology
# fonctionne avec les chaudieres data, classic and HSV  equipées de touchtronic + passerelle internet
# il est possible que ca fontionne sans passerelle (a tester)
# la requete pour créer les tables sont disponibles dans les fichiers create_table_data.sql et create_table_consommation.sql
# prérequis : MysQLdb doit etre installé sur la machine
# optionnel : SQlite3 doit etre installé sur la machine pour activer le mode backup qui copie en local en cas d'indispo de MySQL

# this script is running on raspberry pi
# it listen an Hargassner NanoPK Boiler on telnet
# and then it write data in MySQL or MariaDB on a NAS Synology
# work with data, classic and HSV boiler equiped with touchtronic + internet gateway
# may work without gateway (to be tested)
# to create the database, use the query in createBDD.sql

# Import socket module
import socket               
import time
from datetime import date,datetime,timedelta
import MySQLdb   # MySQLdb must be installed by yourself
import sys
import os.path
import logging
from threading import Thread

#----------------------------------------------------------#
#        parametres                                        #
#----------------------------------------------------------#
DB_SERVER = '192.168.0.111'   # MySQL : IP server (localhost si mySQL est sur la meme machine)
DB_BASE = 'Hargassner'        # MySQL : database name
DB_USER = 'hargassner'        # MySQL : user  
DB_PWD = 'password'           # MySQL : password 
IP_CHAUDIERE = '192.168.0.198'
FIRMWARE_CHAUD = '14g'        # firmware de la chaudiere
PATH_HARG = "/home/pi/hargassner/" #chemin ou se trouve ce script

MODE_BACKUP = True          # True si SQlite3 est installé , sinon False  
INSERT_GROUPED = 1          # regroupe n reception avant d'ecrire en base :INSERT_GROUPED x FREQUENCY = temps en sec
FREQUENCY = 60              # Periodicité (reduit le volume de data mais reduit la précision)
                            # (1 = toutes)     1 mesure chaque seconde
                            # (5)              1 mesure toutes les 5 secondes
                            # ...
                            # une valeur trop faible entraine de gros volume en BDD et surtout des grosses 
                            # lenteurs pour afficher les graphiques : defaut 60sec , evitez de descendre sous les 10 sec
# ne pas modifier ci dessous
MSGBUFSIZE=600
PORT = 23    
backup_row = 0
backup_mode = 0

if FIRMWARE_CHAUD == '14d':
    nbre_param = 174
elif FIRMWARE_CHAUD == '14e':
    nbre_param = 174
elif FIRMWARE_CHAUD == '14f':
    nbre_param = 174
elif FIRMWARE_CHAUD == '14g':
    nbre_param = 190
else:
    nbre_param = 174
   
#----------------------------------------------------------#
#        definition des logs                               #
#----------------------------------------------------------#
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('log')
logger.setLevel(logging.INFO) # choisir le niveau de log : DEBUG, INFO, ERROR...

handler_debug = logging.FileHandler(PATH_HARG + "trace.log", mode="a", encoding="utf-8")
handler_debug.setFormatter(formatter)
handler_debug.setLevel(logging.DEBUG)
logger.addHandler(handler_debug)

#----------------------------------------------------------#
#        socket for Connection to Hargassner               #
#----------------------------------------------------------#
while True:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
        s.connect((IP_CHAUDIERE, PORT))
        logger.info("Creation du socket telnet OK")
        break
    except:
        logger.critical("Connexion a la chaudiere impossible")
        time.sleep(5)
        
#----------------------------------------------------------#
#     definition query with or without BACKUP SQLITE       #
#----------------------------------------------------------#
if MODE_BACKUP == True:
    import sqlite3
    if os.path.isfile(PATH_HARG + 'harg_bck.sqlite3'): # if SQlite exist then resume backup mode
        backup_mode = 1

    # -------------query with BACKUP---------------------------#
    def query_db(sql):
        global backup_mode
        global backup_row
        try:
            db = MySQLdb.connect(DB_SERVER, DB_USER, DB_PWD, DB_BASE)
            cursor = db.cursor()
            #---------------------------------------------------------------#
            #     Normal MySQL database INSERT                              #
            #---------------------------------------------------------------#
            if backup_mode == 0:
                cursor.execute(sql)
                db.commit()
                db.close()
                logger.debug("Ecriture en bdd OK")
            #---------------------------------------------------------------#
            # RESTORE : when MySQL is available again : restore from SQlite #
            #---------------------------------------------------------------#
            else:
                logger.warning('MySQL is OK now : Restore mode started')
                
                db_bck = sqlite3.connect(PATH_HARG + 'harg_bck.sqlite3')
                db_bck.text_factory = str #tell sqlite to work with str instead of unicode
                cursor_bck = db_bck.cursor()

                cursor_bck.execute("""SELECT  * FROM data ORDER BY dateB ASC """) #'DEFAULT' as id,
                result_data = cursor_bck.fetchall ()
               
                for row in result_data:
                    row2 = ('DEFAULT',) + row[1:nbre_param+1] # remplace null par DEFAULT
                    backup_row += 1
                    cursor.execute("""INSERT INTO data VALUES {0}""".format(row2))
                    
                db_bck.close()
                logger.warning('%s rows restored to MySQL', str(backup_row))
               
                backup_row = 0
                backup_mode = 0
                os.remove(PATH_HARG + 'harg_bck.sqlite3')
                logger.warning('Restore done, SQlite3 file deleted, returning to normal mode')
                
                cursor.execute(sql)
                db.commit()
                db.close()

        #---------------------------------------------------------------#
        #     BACKUP : when MySQL is down => local SQlite INSERT        #
        #---------------------------------------------------------------#
        except MySQLdb.Error:
            db_bck = sqlite3.connect(PATH_HARG + 'harg_bck.sqlite3')
            cursor_bck = db_bck.cursor()

            if backup_mode == 0: #create table on first run
                logger.warning('MySQL is down : Backup mode started')
                create_data = """CREATE TABLE IF NOT EXISTS data (
                `id` INT(11) ,
                `dateB` DATETIME NOT NULL,
                `c0` INT(11) NOT NULL,
                `c1` DECIMAL(7,2) NOT NULL,
                `c2` DECIMAL(7,2) NOT NULL,
                `c3` DECIMAL(7,2) NOT NULL,
                `c4` DECIMAL(7,2) NOT NULL,
                `c5` DECIMAL(7,2) NOT NULL,
                `c6` DECIMAL(7,2) NOT NULL,
                `c7` DECIMAL(7,2) NOT NULL,
                `c8` DECIMAL(7,2) NOT NULL,
                `c9` DECIMAL(7,2) NOT NULL,
                `c10` DECIMAL(7,2) NOT NULL,
                `c11` DECIMAL(7,2) NOT NULL,
                `c12` DECIMAL(7,2) NOT NULL,
                `c13` DECIMAL(7,2) NOT NULL,
                `c14` DECIMAL(7,2) NOT NULL,
                `c15` DECIMAL(7,2) NOT NULL,
                `c16` DECIMAL(7,2) NOT NULL,
                `c17` DECIMAL(7,2) NOT NULL,
                `c18` DECIMAL(7,2) NOT NULL,
                `c19` DECIMAL(7,2) NOT NULL,
                `c20` DECIMAL(7,2) NOT NULL,
                `c21` DECIMAL(7,2) NOT NULL,
                `c22` DECIMAL(7,2) NOT NULL,
                `c23` DECIMAL(7,2) NOT NULL,
                `c24` DECIMAL(7,2) NOT NULL,
                `c25` DECIMAL(7,2) NOT NULL,
                `c26` DECIMAL(7,2) NOT NULL,
                `c27` DECIMAL(7,2) NOT NULL,
                `c28` DECIMAL(7,2) NOT NULL,
                `c29` DECIMAL(7,2) NOT NULL,
                `c30` DECIMAL(7,2) NOT NULL,
                `c31` DECIMAL(7,2) NOT NULL,
                `c32` DECIMAL(7,2) NOT NULL,
                `c33` DECIMAL(7,2) NOT NULL,
                `c34` DECIMAL(7,2) NOT NULL,
                `c35` DECIMAL(7,2) NOT NULL,
                `c36` DECIMAL(7,2) NOT NULL,
                `c37` DECIMAL(7,2) NOT NULL,
                `c38` DECIMAL(7,2) NOT NULL,
                `c39` DECIMAL(7,2) NOT NULL,
                `c40` DECIMAL(7,2) NOT NULL,
                `c41` DECIMAL(7,2) NOT NULL,
                `c42` DECIMAL(7,2) NOT NULL,
                `c43` DECIMAL(7,2) NOT NULL,
                `c44` DECIMAL(7,2) NOT NULL,
                `c45` DECIMAL(7,2) NOT NULL,
                `c46` DECIMAL(7,2) NOT NULL,
                `c47` DECIMAL(7,2) NOT NULL,
                `c48` DECIMAL(7,2) NOT NULL,
                `c49` DECIMAL(7,2) NOT NULL,
                `c50` DECIMAL(7,2) NOT NULL,
                `c51` DECIMAL(7,2) NOT NULL,
                `c52` DECIMAL(7,2) NOT NULL,
                `c53` DECIMAL(7,2) NOT NULL,
                `c54` DECIMAL(7,2) NOT NULL,
                `c55` DECIMAL(7,2) NOT NULL,
                `c56` DECIMAL(7,2) NOT NULL,
                `c57` DECIMAL(7,2) NOT NULL,
                `c58` DECIMAL(7,2) NOT NULL,
                `c59` DECIMAL(7,2) NOT NULL,
                `c60` DECIMAL(7,2) NOT NULL,
                `c61` DECIMAL(7,2) NOT NULL,
                `c62` DECIMAL(7,2) NOT NULL,
                `c63` DECIMAL(7,2) NOT NULL,
                `c64` DECIMAL(7,2) NOT NULL,
                `c65` DECIMAL(7,2) NOT NULL,
                `c66` DECIMAL(7,2) NOT NULL,
                `c67` DECIMAL(7,2) NOT NULL,
                `c68` DECIMAL(7,2) NOT NULL,
                `c69` DECIMAL(7,2) NOT NULL,
                `c70` DECIMAL(7,2) NOT NULL,
                `c71` DECIMAL(7,2) NOT NULL,
                `c72` DECIMAL(7,2) NOT NULL,
                `c73` DECIMAL(7,2) NOT NULL,
                `c74` DECIMAL(7,2) NOT NULL,
                `c75` DECIMAL(7,2) NOT NULL,
                `c76` DECIMAL(7,2) NOT NULL,
                `c77` DECIMAL(7,2) NOT NULL,
                `c78` DECIMAL(7,2) NOT NULL,
                `c79` DECIMAL(7,2) NOT NULL,
                `c80` DECIMAL(7,2) NOT NULL,
                `c81` DECIMAL(7,2) NOT NULL,
                `c82` DECIMAL(7,2) NOT NULL,
                `c83` DECIMAL(7,2) NOT NULL,
                `c84` DECIMAL(7,2) NOT NULL,
                `c85` DECIMAL(7,2) NOT NULL,
                `c86` DECIMAL(7,2) NOT NULL,
                `c87` DECIMAL(7,2) NOT NULL,
                `c88` DECIMAL(7,2) NOT NULL,
                `c89` DECIMAL(7,2) NOT NULL,
                `c90` DECIMAL(7,2) NOT NULL,
                `c91` DECIMAL(7,2) NOT NULL,
                `c92` DECIMAL(7,2) NOT NULL,
                `c93` DECIMAL(7,2) NOT NULL,
                `c94` DECIMAL(7,2) NOT NULL,
                `c95` DECIMAL(7,2) NOT NULL,
                `c96` DECIMAL(7,2) NOT NULL,
                `c97` DECIMAL(7,2) NOT NULL,
                `c98` DECIMAL(7,2) NOT NULL,
                `c99` DECIMAL(7,2) NOT NULL,
                `c100` DECIMAL(7,2) NOT NULL,
                `c101` DECIMAL(7,2) NOT NULL,
                `c102` DECIMAL(7,2) NOT NULL,
                `c103` DECIMAL(7,2) NOT NULL,
                `c104` DECIMAL(7,2) NOT NULL,
                `c105` DECIMAL(7,2) NOT NULL,
                `c106` DECIMAL(7,2) NOT NULL,
                `c107` DECIMAL(7,2) NOT NULL,
                `c108` DECIMAL(7,2) NOT NULL,
                `c109` DECIMAL(7,2) NOT NULL,
                `c110` DECIMAL(7,2) NOT NULL,
                `c111` DECIMAL(7,2) NOT NULL,
                `c112` DECIMAL(7,2) NOT NULL,
                `c113` DECIMAL(7,2) NOT NULL,
                `c114` DECIMAL(7,2) NOT NULL,
                `c115` DECIMAL(7,2) NOT NULL,
                `c116` DECIMAL(7,2) NOT NULL,
                `c117` DECIMAL(7,2) NOT NULL,
                `c118` DECIMAL(7,2) NOT NULL,
                `c119` DECIMAL(7,2) NOT NULL,
                `c120` DECIMAL(7,2) NOT NULL,
                `c121` DECIMAL(7,2) NOT NULL,
                `c122` DECIMAL(7,2) NOT NULL,
                `c123` DECIMAL(7,2) NOT NULL,
                `c124` DECIMAL(7,2) NOT NULL,
                `c125` DECIMAL(7,2) NOT NULL,
                `c126` DECIMAL(7,2) NOT NULL,
                `c127` DECIMAL(7,2) NOT NULL,
                `c128` DECIMAL(7,2) NOT NULL,
                `c129` DECIMAL(7,2) NOT NULL,
                `c130` DECIMAL(7,2) NOT NULL,
                `c131` DECIMAL(7,2) NOT NULL,
                `c132` DECIMAL(7,2) NOT NULL,
                `c133` DECIMAL(7,2) NOT NULL,
                `c134` DECIMAL(7,2) NOT NULL,
                `c135` DECIMAL(7,2) NOT NULL,
                `c136` DECIMAL(7,2) NOT NULL,
                `c137` DECIMAL(7,2) NOT NULL,
                `c138` DECIMAL(7,2) NOT NULL,
                `c139` DECIMAL(7,2) NOT NULL,
                `c140` DECIMAL(7,2) NOT NULL,
                `c141` DECIMAL(7,2) NOT NULL,
                `c142` DECIMAL(7,2) NOT NULL,
                `c143` DECIMAL(7,2) NOT NULL,
                `c144` DECIMAL(7,2) NOT NULL,
                `c145` DECIMAL(7,2) NOT NULL,
                `c146` DECIMAL(7,2) NOT NULL,
                `c147` DECIMAL(7,2) NOT NULL,
                `c148` DECIMAL(7,2) NOT NULL,
                `c149` DECIMAL(7,2) NOT NULL,
                `c150` DECIMAL(7,2) NOT NULL,
                `c151` DECIMAL(7,2) NOT NULL,
                `c152` DECIMAL(7,2) NOT NULL,
                `c153` DECIMAL(7,2) NOT NULL,
                `c154` DECIMAL(7,2) NOT NULL,
                `c155` DECIMAL(7,2) NOT NULL,
                `c156` DECIMAL(7,2) NOT NULL,
                `c157` DECIMAL(7,2) NOT NULL,
                `c158` DECIMAL(7,2) NOT NULL,
                `c159` DECIMAL(7,2) NOT NULL,
                `c160` DECIMAL(7,2) NOT NULL,
                `c161` DECIMAL(7,2) NOT NULL,
                `c162` DECIMAL(7,2) NOT NULL,
                `c163` DECIMAL(7,2) NOT NULL,
                `c164` DECIMAL(7,2) NOT NULL,
                `c165` DECIMAL(7,2) NOT NULL,
                `c166` DECIMAL(7,2) NOT NULL,
                `c167` DECIMAL(7,2) NOT NULL,
                `c168` DECIMAL(7,2) NOT NULL,
                `c169` DECIMAL(7,2) NOT NULL,
                `c170` DECIMAL(7,2) NOT NULL,
                `c171` DECIMAL(7,2) NOT NULL,
                `c172` DECIMAL(7,2) NOT NULL,
                `c173` DECIMAL(7,2) NOT NULL,
                `c174` DECIMAL(7,2) NOT NULL,
                `c175` DECIMAL(7,2) NOT NULL,
                `c176` DECIMAL(7,2) NOT NULL,
                `c177` DECIMAL(7,2) NOT NULL,
                `c178` DECIMAL(7,2) NOT NULL,
                `c179` DECIMAL(7,2) NOT NULL,
                `c180` DECIMAL(7,2) NOT NULL,
                `c181` DECIMAL(7,2) NOT NULL,
                `c182` CHAR(5) NOT NULL,
                `c183` DECIMAL(7,2) NOT NULL,
                `c184` DECIMAL(7,2) NOT NULL,
                `c185` DECIMAL(7,2) NOT NULL,
                `c186` DECIMAL(7,2) NOT NULL,
                `c187` DECIMAL(7,2) NOT NULL,
                `c188` DECIMAL(7,2) NOT NULL
                ) ;"""

                cursor_bck.execute(create_data)
                logger.warning('SQlite created')

            backup_mode = 1
            cursor_bck.execute(sql)
            db_bck.commit()
            db_bck.close()
else:   
    # -------------query without BACKUP------------------------#
    def query_db(sql):
        try:
            db = MySQLdb.connect(DB_SERVER, DB_USER, DB_PWD, DB_BASE)
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            db.close()
            logger.debug("Ecriture en bdd OK")
            
        except MySQLdb.Error:
            logger.error("MySQL is down : %s", MySQLdb.Error)
    
#----------------------------------------------------------#
#             initialisation table consommation            #
#             au 1er lancement du script
#             si la table est vide on rempli une ligne a vide
#----------------------------------------------------------#
try:
    db = MySQLdb.connect(DB_SERVER, DB_USER, DB_PWD, DB_BASE)
    cursor = db.cursor()

    try:
        cursor.execute("""SELECT COUNT(dateB) FROM consommation """)
        compt = cursor.fetchone ()
        if compt[0] == 0:
            dateH =  date.today() + timedelta(days=-1)
            cursor.execute("""INSERT INTO consommation (dateB, conso, Tmoy) VALUES ('%s','%s','%s')""" % (dateH,'0','0'))
    except:
        logger.error('lecture/ecriture table consommation impossible')

    db.commit()
    db.close()
except:
    logger.error('Erreur initialisation table consommation')

#----------------------------------------------------------#
#             declaration threads                          #
#----------------------------------------------------------#

#################################################################
# toutes les 2h ce thread verifie si on change de journée
# et calcul la conso de la veille avant de l'ecrire dans la table consommation
def thread_consommation():
    while True:
        try:
            db = MySQLdb.connect(DB_SERVER, DB_USER, DB_PWD, DB_BASE)
            cursor = db.cursor()
            
            cursor.execute("""SELECT dateB FROM consommation
                            ORDER by dateB DESC LIMIT 1 """)
            result = cursor.fetchone ()
            last_conso = result[0] + timedelta(days=1)
           
            if date.today() > last_conso:
                cursor.execute("""SELECT DATE(dateB),MAX(c99)-MIN(c99),FORMAT(AVG(c6), 1) FROM data
                                GROUP BY DATE(dateB)
                                ORDER by dateB DESC LIMIT 1,1 """)
                result = cursor.fetchone ()
                cursor.execute("""INSERT INTO consommation (dateB, conso, Tmoy) VALUES ('%s','%s','%s')""" % (result[0],result[1],result[2]))

            db.commit()
            db.close()
        except:
            logger.error('Erreur dans le Thread consommation')
        time.sleep(7200)


#################################################################
# ce thread ecoute la chaudiere qui emet toute les 1 seconde
# la suite du programme vient piocher la valeur du buffer quand elle en a besoin (60sec par defaut).
# cette methode est plus efficace que d'ecouter la chaudiere uniquement quand on a besoin
# car on tombe sur des buffer en cours d'emission(incomplet) ce qui genere beaucoup d'erreur
def thread_buffer():
    global bufferOK
    while True:
        try:
            buffer = s.recv(MSGBUFSIZE) # waiting a packet (waiting as long as s.recv is empty)
            if buffer[0:2] == "pm":
                bufferOK = buffer
            else:
                logger.debug('buffer ERREUR pm')
        except:
            logger.error('buffer ERREUR lecture')
        # except KeyboardInterrupt:
            # thread1._Thread__stop()
            # break

## execution thread parallele#############################################

thread1 = Thread(target=thread_buffer)
thread2 = Thread(target=thread_consommation)
thread1.start()
thread2.start()
time.sleep(5) #laisse le temps au buffer de se remplir
    
#----------------------------------------------------------#
#             suite du programme                           #
#----------------------------------------------------------#
    
i=0
tableau = []
#------preparation requete----------
list_champ = ",'%s'" * nbre_param
requete = "INSERT INTO data  VALUES (null" + list_champ + ")" # null correspond au champ id
 
while True:
    try:
        if bufferOK[0:2] == "pm":
            datebuff = time.strftime('%Y-%m-%d %H:%M:%S') #formating date for mySQL
            buff_liste=bufferOK.split()    # transforme la string du buffer en liste 
            logger.debug(buff_liste)
            buff_liste[0] = datebuff       # remplace la valeur "pm" par la date
            list_liste = buff_liste [0:nbre_param]# selectionne les valeurs voulues, la valeur (nbre_param)doit correspondre au nombre de %s ci dessous
            tupl_liste = tuple(list_liste) # transforme la liste en tuple (necessaire pour le INSERT)
            tableau.append(tupl_liste)     # cumule les tuples dans un tableau
            i = i + 1
            try:
                if i == INSERT_GROUPED:
                    tableau = tuple(tableau)  # crée un tuple de tuple
                    for x in range(INSERT_GROUPED):
                        query_db( requete % tableau[x] ) 
                    
                    logger.debug('write DB : %s', tableau[0][0])
                    i = 0
                    tableau = []
                time.sleep(FREQUENCY - 1)
            except:
                logger.error('insert KO')
        else:
            logger.debug(bufferOK)

    except :
        logger.error('le if pm est KO, buffer non defini')
        time.sleep(1)
        continue

        
def fermeture(signal,frame):
    # arret du script 
    thread1._Thread__stop()
    thread2._Thread__stop()
    s.close()   

# interception du signal d'arret du service   pour fermer les threads  
signal.signal(signal.SIGTERM, fermeture) 
        
