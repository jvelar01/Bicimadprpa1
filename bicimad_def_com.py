import json
from pprint import pprint
from pyspark import SparkContext, SparkConf
import datetime
import sys
import os
import matplotlib.pyplot as plt

#establecer estacion, distrito y filtros (tambien se puede con sys.argv)
YEAR = 2017
ESTACION= 'Primavera'
DISTRITO=[i for i in range(64,92)] #retiro
QUIERO_FILTRAR_DISTRITO=True
QUIERO_FILTRAR_ESTACION=False
ANALISIS_DE_AÑO=False

#funcion para extraer informacion de cada linea
def info_line(line):
    data = json.loads(line)
    ageRange1 = data['ageRange']
    id1 = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    day = data['unplug_hourTime']['$date']
    duracion=data["travel_time"]
    tipo_usuario=data['user_type']
    return (ageRange1, id1, start, end, date_traductor(day),duracion, tipo_usuario)

#traduccion de fechas
def date_traductor(date):
    """traduce la fecha recibida de los archivos de bicimad
    devuelve una tupla con la fecha en formato tupla
    la tupla contiene el dia de la semana, el dia del mes, el mes y el año."""
    
    month = int(date[5:7])
    day = int(date[8:10])
    week_day = datetime.datetime(YEAR, month, day).weekday()
    return (week_day, day, month, YEAR)


def day_traductor(week_day):
    """dado un weekday del modulo datetime, esto es, un numero del 0 al 6, 
    traduce el numero a lenguaje natural"""
    
    if week_day == 0:
        name_day = 'Lunes'
    elif week_day == 1:
        name_day = 'Martes'
    elif week_day == 2:
        name_day = 'Miércoles'
    elif week_day == 3:
        name_day = 'Jueves'
    elif week_day == 4:
        name_day = 'Viernes'
    elif week_day == 5:
        name_day = 'Sábado'
    else:
        name_day = 'Domingo'
    return name_day

def season(date_tuple):
    """dada una tupla devuelta por date_traductor, hace un return de la  
    estacion en que esta esa fecha"""
    
    (name, day, month, year) = date_tuple
    if month == 4 or month == 5 or (month == 3 and day >=20) or (month == 6 and day <=20):
        season = 'Primavera'
    elif month == 7 or month == 8 or (month == 6 and day >=21) or (month == 9 and day <=21):
        season = 'Verano'
    elif month == 10 or month == 11 or (month == 9 and day >=22) or (month == 12 and day <=20):
        season = 'Otoño'
    else:
        season = 'Invierno'
    return season

#funciones relacionadas con el filtraje de la informacion antes del analisis

def want_filter_season():
    return QUIERO_FILTRAR_ESTACION

def want_filter_district():
    return QUIERO_FILTRAR_DISTRITO
        
def is_in_district(x):
    return x in DISTRITO

def filter_district(rdd):
    rdd_distrito=rdd.filter(lambda x: is_in_district(int(x[2])))
    return rdd_distrito

def filter_season(rdd):
    rdd_season=rdd.filter(lambda x : season(x[4]) == ESTACION)
    return rdd_season


#filtros auxiliares para las funciones principales

#filtros de edad

def filter_age_start(rdd, age):
    """dada una edad, devuelve un rdd en el que solo estan los viajes 
    hechos por usuarios de esa edad, con la salida como key, las demas funciones
    de esta seccion son analogas"""
    
    rdd_age = rdd.filter(lambda x : x[0] == age).map(lambda x: (x[2], (x[1], x[3], x[4]))) 
    return rdd_age

def filter_age_end(rdd, age):
    rdd_age = rdd.filter(lambda x : x[0] == age).map(lambda x: (x[3], (x[1], x[2], x[4]))) #devuelve el end como key
    return rdd_age

#funciones para filtrar por tipo de usuario

def filter_type_start(rdd,tipo):
    rdd_tipo=rdd.filter(lambda x : x[6] == tipo).map(lambda x: (x[2], (x[1], x[3], x[4])))
    return rdd_tipo

def filter_type_end(rdd, tipo):
    rdd_tipo=rdd.filter(lambda x : x[6] == tipo).map(lambda x: (x[3], (x[1], x[2], x[4])))
    return rdd_tipo

#funciones para filtrar por dia de la semana

def filter_day_start(rdd, day):
    rdd_day = rdd.filter(lambda x : x[4][0] == day).map(lambda x: (x[2], (x[1], x[3], x[4])))
    return rdd_day

def filter_day_end(rdd, day):
    rdd_day = rdd.filter(lambda x : x[4][0] == day).map(lambda x: (x[3], (x[1], x[2], x[4])))
    return rdd_day


#fin de filtros auxiliares


#funcion que devuelve la estacion con mas salidas para cada dia de la semana
def spot_more_starts_per_day(rdd):
    rdd_day = [0] * 7
    for i in range(7):
        rdd_day[i] = filter_day_end(rdd, i).countByKey()
        max_est = max(rdd_day[i], key = rdd_day[i].get)
        print(f'La estación de la que salen más bicicletas para los {day_traductor(i)} es {max_est} y han salido {rdd_day[i][max_est]} bicis')

#funcion que devuelve la estacion con mas llegadas para cada dia de la semana
def spot_more_ends_per_day(rdd):
    rdd_day = [0] * 7
    for i in range(7):
        rdd_day[i] = filter_day_start(rdd, i).countByKey()
        max_est = max(rdd_day[i], key = rdd_day[i].get)
        print(f'La estación en la que llegan más bicicletas para los {day_traductor(i)} es {max_est} y han llegado {rdd_day[i][max_est]} bicis')

#funcion que devuelve la estacion con mas salidas para cada tipo de usuario                
def spot_more_starts_per_type(rdd):
    rdd_type = [0] * 4
    for i in range(4):
        rdd_type[i] = filter_type_start(rdd, i).countByKey()
        if len(list(rdd_type[i]))>1:
            max_est = max(rdd_type[i], key = rdd_type[i].get)
            print(f'La estación de la que salen más bicicletas para el tipo de usuario {i} es {max_est} y han salido {rdd_type[i][max_est]} bicis')    
        else:
            print(f'para el tipo de usuario {i} no salen bicis')

#funcion que devuelve la estacion con mas llegadas para cada tipo de usuario.        
def spot_more_ends_per_type(rdd):
    rdd_type = [0] * 4
    for i in range(4):
        rdd_type[i] = filter_type_start(rdd, i).countByKey()
        if len(list(rdd_type[i]))>1:
            max_est = max(rdd_type[i], key = rdd_type[i].get)
            print(f'La estación en la que llegan más bicicletas para el tipo de usuario {i} es {max_est} y han salido {rdd_type[i][max_est]} bicis')
        else:
            print(f'para el tipo de usuario {i} no llegan bicicletas')

#analogo a las anteriores pero para cada rango de edad. 
def spot_more_starts_per_age(rdd):
    rdd_age = [0] * 7
    for i in range(7):
        rdd_age[i] = filter_age_start(rdd, i).countByKey()
        if len(list(rdd_age[i]))>1:
            max_est = max(rdd_age[i], key = rdd_age[i].get)
            print(f'La estación de la que salen más bicicletas para el grupo de edad {i} es {max_est} y han salido {rdd_age[i][max_est]} bicis')
        else:
            print(f'para el rango de edad {i} no salen bicicletas')
 
def spot_more_ends_per_age(rdd):
    rdd_age = [0] * 7
    for i in range(7):
        rdd_age[i] = filter_age_end(rdd, i).countByKey()
        if len(list(rdd_age[i]))>1:
            max_est = max(rdd_age[i], key = rdd_age[i].get)
            print(f'La estación en la que más bicicletas para el grupo de edad {i} es {max_est} y han llegado {rdd_age[i][max_est]} bicis') 
        else:
            print(f'para el rango de edad {i} no llegan bicicletas')

 

#funcion que imprime por pantalla cuantos viajes se hacen por edad        
def trips_per_age(rdd): 
    rdd_counted_ages = rdd.countByKey()
    print('Los grupos de edades y las bicicletas que utilizan son:', '\n')
    for i in rdd_counted_ages:
        print(f'El grupo de edad {i} usó {rdd_counted_ages[i]} bicicletas')
        
        

     #GRÁFICA DE BARRAS 
    ejes = [[0,1,2,3,4,5,6],[rdd_counted_ages[i] for i in range(7)]]
    plt.bar(ejes[0],ejes[1])
    plt.ylabel('Número de usuarios')
    plt.xlabel('Rangos de Edad')
    plt.title('Número de usuarios dependiendo de la edad')
    plt.savefig('viajesedadbarras',format='png')        
    
    
    #GRAFICO DE SECTORES 
    labels = ejes[0]
    sizes = ejes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')  
    plt.savefig('viajesedadsector',format='png')        

        
#funcion que imprime por pantalla el tiempo medio de cada viaje por edad.            
def time_per_age(rdd_base):
    rdd_duracion= rdd_base.map(lambda x:(x[0],x[5])).groupByKey().map(lambda x : (sum(list(x[1]))/len(list(x[1])))).collect()
    print('la media de tiempo que se utilizan las bicicletas según el rango de edad es:')
    for i in range(len(list(rdd_duracion))):
        print(f'El grupo de edad {i} usó de media {rdd_duracion[i]/60} minutos las bicicletas')
      
    #GRÁFICA DE BARRAS 
    ejes = [[0,1,2,3,4,5,6],list(rdd_duracion)]
    plt.bar(ejes[0],ejes[1])
    plt.ylabel('tiempo medio del usuario')
    plt.xlabel('edad del usuario')
    plt.title('tiempo medio del usuario dependiendo del la edad del usuario')
    plt.savefig('tiempoedad',format='png')        
        
  
#funcion que imprime por pantalla cuantos viajes se hacen por tipo de usuario       
def trips_per_type(rdd): 
    rdd_counted_type = rdd.map(lambda x: (x[6],(x[0:5]))).countByKey()
    print('Los tipos de usuarios y las bicicletas que utilizan son:', '\n')  
    for i in rdd_counted_type:
        print(f'el tipo de usuario {i} usó {rdd_counted_type[i]} bicicletas')    
        
     #GRÁFICA DE BARRAS 
    ejes = [[1,2,3],[rdd_counted_type[i] for i in range(1,4)]]
    plt.bar(ejes[0],ejes[1])
    plt.ylabel('Número de usuarios')
    plt.xlabel('tipos de usuarios')
    plt.title('Número de usuarios dependiendo del tipo de usuario')
    plt.savefig('viajestiposgraf',format='png')        
    
    
    #GRAFICO DE SECTORES 
    labels = ejes[0]
    sizes = ejes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')  
    plt.savefig('viajestiposector',format='png')        
        
    
#funcion que imprime por pantalla el tiempo medio de cada viaje por tipo de usuario    
def time_per_type(rdd_base):
    rdd_duracion= rdd_base.map(lambda x:(x[6],x[5])).groupByKey().map(lambda x : (sum(list(x[1]))/len(list(x[1])))).collect()
    print('la media de tiempo que se utilizan las bicicletas según el tipo de usuario es:')
    for i in range(len(list(rdd_duracion))):
        print(f'El tipo de usuario {i+1} usó de media {rdd_duracion[i]/60} minutos las bicicletas')     
    
    #GRÁFICA DE BARRAS 
    ejes = [[1,2,3],[rdd_duracion[i] for i in range(3)]]
    plt.bar(ejes[0],ejes[1])
    plt.ylabel('tiempo medio del tipo de usuario')
    plt.xlabel('tipo de usuario')
    plt.title('tiempo medio del usuario dependiendo del tipo de usuario')
    plt.savefig('duratipo',format='png')
    
  
#funcion que imprime por pantalla la cantidad de viajes  que se hacen al mes, en caso de analisis anual 
def trips_per_month(rdd):
    rdd_trips=rdd.map(lambda x: (x[4][2], (x[1], x[3], x[4]))).countByKey()
    for i in range(1,13):
        print(f'en el mes {i} del año se utilizaron {rdd_trips[i]} bicicletas')     
    ejes = [[i for i in range(1, 13)],[rdd_trips[i] for i in range(1, 13)]]
    plt.bar(ejes[0],ejes[1])
    plt.ylabel('Número de viajes')
    plt.xlabel('mes del año')
    plt.title('Número de viajes dependiendo del mes')
    plt.savefig('viajesaño',format='png')
       



def main():
    
    #inicio de spark y lectura de los archivos json
    
    conf = SparkConf().setAppName("Rutas")
    with SparkContext(conf = conf) as sc:
        sc.setLogLevel("ERROR")
 
        direct = os.path.abspath('bicimad_data')
        rdd_base = sc.emptyRDD()
        i=0
        for filename in os.listdir(direct):
            i=i+1
            if  filename.endswith(".json"):
                print(filename)
                file_rdd = sc.textFile(os.path.join(direct, filename))
                rdd_base = rdd_base.union(file_rdd)
        
            else:
                pass
        
        if i==12:
            ANALISIS_DE_AÑO=True

        rdd = rdd_base.map(info_line)
        
        #aplicacion de filtros

        if want_filter_district():
            rdd=filter_district(rdd)
            print('\n','analisis de los datos del distrito','\n')
        else:
            print('\n','analisis de los datos de Madrid','\n')
            
            
        if want_filter_season():
            print('se filtraran los datos por epoca del año ')
            rdd=filter_season(rdd)
        else:
            print('\n','no se filtraran los datos por epoca del año')
        
       
        #impresion por pantalla del analisis
        
        print('\n', 'Estadísticas de salida y entrada de bicicletas por días', '\n')
        
        spot_more_starts_per_day(rdd)
        print('\n')
        spot_more_ends_per_day(rdd)
        
        
        print('\n', 'Estadísticas de salida y entrada de bicicletas por tipo de usuario', '\n')
        print('Los tipos de usuarios son:', '\n')   
        print('0: No se ha podido determinar el tipo de usuario', '\n') 
        print('1: Usuario anual (poseedor de un pase anual', '\n') 
        print('2: Usuario ocasional', '\n') 
        print('3: Trabajador de la empresa', '\n')
    
        spot_more_starts_per_type(rdd)
        print('\n')
        spot_more_ends_per_type(rdd)
        
        print('\n', 'Estadísticas de salida y entrada de bicicletas por edades', '\n')
        print('Los grupos de edades son:', '\n')
        print('0: No se ha podido determinar el rango de edad del usuario' , '\n')
        print ('1: El usuario tiene entre 0 y 16 años' , '\n')
        print ('2: El usuario tiene entre 17 y 18 años', '\n')
        print ('3: El usuario tiene entre 19 y 26 años' , '\n')
        print ('4: El usuario tiene entre 27 y 40 años' , '\n')
        print('5: El usuario tiene entre 41 y 65 años' , '\n')
        print('6: El usuario tiene 66 años o más' , '\n')
        
        spot_more_starts_per_age(rdd)
        print('\n')
        spot_more_ends_per_age(rdd)
    
        print('\n', 'Uso según la edad', '\n')
    
        trips_per_age(rdd)
        time_per_age(rdd)
        
        
        print('\n', 'Estadísticas de uso de bicicletas por tipo de usuario', '\n')
        
        trips_per_type(rdd)  
        time_per_type(rdd)
        
        if ANALISIS_DE_AÑO:
            print('Evolución de usuarios anual')
            trips_per_month(rdd)
    sc.stop()

if __name__ == "__main__":
    
    #interaccion con el usuario en caso de que lo desee para cambiar los filtros iniciales.
    if len(sys.argv) > 2:
        if isinstance(sys.argv[1], bool):
            if sys.argv[1]:
                QUIERO_FILTRAR_ESTACION=True
            else:
                QUIERO_FILTRAR_ESTACION=False
                
        if isinstance(sys.argv[2], str):
            ESTACION=sys.argv[2]
            
        if isinstance(sys.argv[3], bool):
            if sys.argv[3]:
                QUIERO_FILTRAR_DISTRITO=True
            else:
                QUIERO_FILTRAR_DISTRITO=False
                
        if isinstance(sys.argv[4], list):
            DISTRITO=sys.argv[4]
            
        
    main()



