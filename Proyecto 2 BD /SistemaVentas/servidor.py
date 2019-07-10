import ssl
import sys
import psycopg2 #conectarte python con postresql
import paho.mqtt.client #pip install paho-mqtt
import json

conn = psycopg2.connect(host = 'raja.db.elephantsql.com', user= 'oyoqynnr', password ='myHVlpJkEO21o29GKYSvMCGI3g4y05bh', dbname= 'oyoqynnr')

    
def on_connect(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='unimet/#', qos = 0)        


def ventasTIENDA(client, userdata, message):   
    a = json.loads(message.payload) 
    print(a) 
    cur = conn.cursor()
    sql = '''INSERT INTO ventas (time_stamp, id_tienda, mac_add, monto) VALUES ( %s, %s, %s, %s);'''
    cur.execute(sql, (a["DATE"],a["ID_TIENDA"],a["MAC_ADD"],a["MONTO"]))
    conn.commit()
    print('VENTA EFECTUADA')
    print('------------------------------')   




def main():	
    client = paho.mqtt.client.Client()
    client.on_connect = on_connect
    client.message_callback_add('unimet/ventas', ventasTIENDA)
    client.connect("broker.hivemq.com",1883,60)
    client.loop_forever()

if __name__ == '__main__':
	main()
	sys.exit(0)



