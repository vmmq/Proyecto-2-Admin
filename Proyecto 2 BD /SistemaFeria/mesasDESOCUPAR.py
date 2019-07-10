import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
import psycopg2

conn = psycopg2.connect(host = 'raja.db.elephantsql.com', user= 'gbhreyfu', password ='8p6HhwudtS1TAf5QjTnxhmYlWNmcOYiz', dbname= 'gbhreyfu') #sistema accesos
conn2 = psycopg2.connect(host = 'raja.db.elephantsql.com', user= 'hmwfspfz', password ='pT0BY4NzZCka8tuAEeHV5JR3ZJHhU8mf', dbname= 'hmwfspfz') #sistema mesas

def on_connect(client, userdata, flags, rc):
	print('conectado publicador')

def main():
	client = paho.mqtt.client.Client()
	client.qos = 0
	client.connect("broker.hivemq.com",1883,60)
	meanEntrada = 300 
	stdEntrada = 30
	cantCamaras = 20

	stdEntrada= 1/((stdEntrada+meanEntrada)/(60*60))
	meanEntrada = 1/(meanEntrada/(60*60))
	stdEntrada = meanEntrada-stdEntrada

	print (meanEntrada)
	print (stdEntrada)



	while(True):
		hora = datetime.datetime.now()	
		cursor = conn.cursor()


		
		cursor2 = conn2.cursor()

		postgreSQL_select_Query = "SELECT id, id_mesa, estado, referencia, mac_add FROM mesa_log WHERE id IN ( SELECT MAX(id) FROM mesa_log GROUP BY id_mesa) AND estado = 1"
		cursor2.execute(postgreSQL_select_Query)
		mobile_records = cursor2.fetchall()
		
		
		if len(mobile_records) >= 1:
			persona_saliente =mobile_records[int(np.random.uniform(0, len(mobile_records)))]


			

			print("Id Detectado= ", persona_saliente[0], )
			estado = 0
			mac_id = persona_saliente[4]
			id = persona_saliente[3]
			mesa = persona_saliente[1]
			#marcar ocupado o desocupado segun estado
			sql_update_query = "Update log_cc set ocupado = %s where id = %s"

			cursor.execute(sql_update_query, (0, id))
			
			conn.commit()

			
				
			payload = {
					"DATE": str(hora),
					"MESA_ID": str(mesa),
					"MAC_ADD": mac_id,
					"ESTADO": estado,
					"ID": id,
			}
			client.publish('unimet/mesa',json.dumps(payload),qos=0)		

			print(payload)


		
			time.sleep(int(np.random.normal(meanEntrada, stdEntrada)))



if __name__ == '__main__':
	main()
	sys.exit(0)


