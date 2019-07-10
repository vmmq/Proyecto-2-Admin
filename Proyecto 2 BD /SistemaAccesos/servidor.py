import ssl
import sys
import psycopg2 #conectarte python con postresql
import paho.mqtt.client #pip install paho-mqtt
import json

conn = psycopg2.connect(host = 'raja.db.elephantsql.com', user= 'gbhreyfu', password ='8p6HhwudtS1TAf5QjTnxhmYlWNmcOYiz', dbname= 'gbhreyfu')

    
def on_connect(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='unimet/#', qos = 0)        

def entradasCC(client, userdata, message):   
    a = json.loads(message.payload) 
    print(a) 
    #print(message.qos)        
    if a["TYPE"] == "ENTRADA":
        cur = conn.cursor()
        sql = '''INSERT INTO log_cc (time_stamp, id_camara, mac_add, gender, age, count) VALUES ( %s, %s, %s, %s, %s, %s);'''
        cur.execute(sql, (a["DATE"],a["CAM_ID"],a["MAC_ADD"],a["GENDER"],a["AGE"],1))
        conn.commit()
        print('NUEVA ENTRADA CC')
    print('------------------------------')   

    if a["TYPE"] == "SALIDA":
        cur = conn.cursor()
        sql = '''INSERT INTO log_cc (time_stamp, id_camara, mac_add, gender, age, count,out,ocupado) VALUES ( %s, %s, %s, %s, %s, %s,%s, %s);'''
        cur.execute(sql, (a["DATE"],a["CAM_ID"],a["MAC_ADD"],a["GENDER"],a["AGE"],-1,1,1))
        conn.commit()
        print('NUEVA SALIDA CC')
    print('------------------------------')   


def entradasTIENDA(client, userdata, message):   
    a = json.loads(message.payload) 
    print(a) 
    #print(message.qos)        
    if a["TYPE"] == "ENTRADA":
        cur = conn.cursor()
        sql = '''INSERT INTO log_tienda (time_stamp, id_tienda, mac_add, count, referencia) VALUES ( %s, %s, %s, %s, %s);'''
        cur.execute(sql, (a["DATE"],a["TIENDA_ID"],a["MAC_ADD"],1,a["ID"]))
        conn.commit()
        print('NUEVA ENTRADA TIENDA')
    print('------------------------------')   

    if a["TYPE"] == "SALIDA":
        cur = conn.cursor()
        sql = '''INSERT INTO log_tienda (time_stamp, id_tienda, mac_add, count,out, referencia) VALUES ( %s, %s, %s, %s, %s, %s);'''
        cur.execute(sql, (a["DATE"],a["TIENDA_ID"],a["MAC_ADD"],-1,1,a["ID"]))
        conn.commit()

        cursor = conn.cursor()
        sql_update_query = "Update log_cc set ocupado = %s where id = %s"
        cursor.execute(sql_update_query, (0, a["ID"]))
        conn.commit()
        
        print('NUEVA SALIDA TIENDA')
    print('------------------------------')   




def main():	
    client = paho.mqtt.client.Client()
    client.on_connect = on_connect
    client.message_callback_add('unimet/acceso/cc', entradasCC)
    client.message_callback_add('unimet/acceso/tienda', entradasTIENDA)

    client.connect("broker.hivemq.com",1883,60)
    client.loop_forever()

if __name__ == '__main__':
	main()
	sys.exit(0)



