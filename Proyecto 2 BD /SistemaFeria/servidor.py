import ssl
import sys
import psycopg2 #conectarte python con postresql
import paho.mqtt.client #pip install paho-mqtt
import json

conn = psycopg2.connect(host = 'raja.db.elephantsql.com', user= 'hmwfspfz', password ='pT0BY4NzZCka8tuAEeHV5JR3ZJHhU8mf', dbname= 'hmwfspfz')

    
def on_connect(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='unimet/#', qos = 0)        
 

def mesaFERIA(client, userdata, message):   
    if len(message.payload) > 10:
        a = json.loads(message.payload) 
        print(a) 
        cur = conn.cursor()
        sql = '''INSERT INTO mesa_log (time_stamp, id_mesa, mac_add, estado,referencia) VALUES ( %s, %s, %s, %s,%s);'''
        cur.execute(sql, (a["DATE"],a["MESA_ID"],a["MAC_ADD"],a["ESTADO"],a["ID"]))
        conn.commit()
        print('CAMBIO EN MESA')
        print('------------------------------')   
    else:
        print(message.payload)
        msg = str(message.payload)
        msg.split("'")

        mesa_id = msg[2]+msg[3]
        estado = msg[5]

        cur = conn.cursor()
        sql = '''INSERT INTO mesa_log (id_mesa, estado) VALUES ( %s, %s);'''
        cur.execute(sql, (mesa_id,estado))
        conn.commit()
        print('CAMBIO EN MESA')
        print('------------------------------')   




def main():	
    client = paho.mqtt.client.Client()
    client.on_connect = on_connect
    client.message_callback_add('unimet/mesa', mesaFERIA)
    client.connect("broker.hivemq.com",1883,60)
    client.loop_forever()

if __name__ == '__main__':
	main()
	sys.exit(0)



