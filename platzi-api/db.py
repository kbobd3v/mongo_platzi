from bson.json_util import dumps, ObjectId
from flask import current_app
from pymongo import MongoClient, DESCENDING
from werkzeug.local import LocalProxy


# Este método se encarga de configurar la conexión con la base de datos
def get_db():
    platzi_db = current_app.config['PLATZI_DB_URI']
    client = MongoClient(platzi_db)
    return client.platzi


# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)


def test_connection():
    return dumps(db.collection_names())


def collection_stats(collection_nombre):
    return dumps(db.command('collstats', collection_nombre))

# -----------------Carreras-------------------------

# La función crear carrera recibe como parametro el json que llega desde la api rest
# luego inserta la información del json en la coleccion carreras y devuelve el id de la carrera insertada
def crear_carrera(json):
    return str(db.carreras.insert_one(json).inserted_id)

# la función consultar carrera por id recibe como parametro el id de la carrera que queremos buscar en la base de datos
# usamos el metodo dumps que convierte el bson a json
# usamos el comando find one donde agregamos el _id como string y en el ObjectId pasamos el id de la carrera que viene del json
def consultar_carrera_por_id(carrera_id):
    return dumps(db.carreras.find_one({'_id': ObjectId(carrera_id)}))

# Actualizar carrera recibe un json de carrera,en postman debe modificarse el id que viene en el json por el id de la carrera que creamos anteriormente
# para que el filtro lo encuentre y ejecute los cambios
def actualizar_carrera(carrera):
    # Esta funcion solamente actualiza nombre y descripcion de la carrera
    return str(db.carreras.update_one({'_id': ObjectId(carrera['_id'])}, {'$set': {'nombre': carrera['nombre'], 'descripcion': carrera['descripcion']}}).modified_count)

# Borrar carrera toma especificamente carrera_id y lo convierte en el ObjectId para usarlo como filtro y asi poder eliminar el documento
def borrar_carrera_por_id(carrera_id):
    return str(db.carreras.delete_one({'_id': ObjectId(carrera_id)}))


# Clase de operadores
def consultar_carreras(skip, limit):
    return dumps(db.carreras.find({}).skip(int(skip)).limit(int(limit)))

# Agregar curso a carrera toma nuestro json como parametro
# Declara una variable curso que filtra el curso que queremos agregar, por defecto trae el id del curso y en la proyeccion pedimos que traiga el nombre
# luego ejecuta la actualización de la carrera filtrando la carrera que queremos editar
# y con  $addToSet agregamos la información del curso al documento de la carrera en el arreglo de cursos, por eso usamos el operador $addToSet
def agregar_curso(json):
    curso = consultar_curso_por_id_proyeccion(json['id_curso'], proyeccion={'nombre':1})
    return str(db.carreras.update_one({'_id': ObjectId(json['id_carrera'])}, {'$addToSet': {'cursos': curso}}).modified_count)


def borrar_curso_de_carrera(json):
    return str(db.carreras.update_one({'_id': ObjectId(json['id_carrera'])}, {'$pull':{'cursos': {'_id': ObjectId(json['id_curso'])}}}).modified_count)

# -----------------Cursos-------------------------

# Crear curso recibe un json de parametro que contiene el documento que queremos agregar a la colección de cursos
# retorna el id del curso creado
def crear_curso(json):
    return str(db.cursos.insert_one(json).inserted_id)

# Consultar curso recibe el id_curso como parametro y ejecuta la busqueda agregando id_curso dentro del objectid del filtro
def consultar_curso_por_id(id_curso):
    return dumps(db.cursos.find_one({'_id': ObjectId(id_curso)}))

# Actualizar curso toma el json curso y realiza una busqueda en la base de datos de un curso determinado por el id y luego con $set modifica la información del curso
# Y retorna la cantidad de documentos modificados
def actualizar_curso(curso):
    # Esta funcion solamente actualiza nombre, descripcion y clases del curso
    filter = {"_id": ObjectId(curso["_id"])}
    set = {'$set': {'nombre': curso['nombre'], 'descripcion': curso['descripcion'], 'clases': curso['clases']}}
    return str(db.cursos.update_one(filter, set).modified_count)

def borrar_curso_por_id(curso_id):
    return str(db.cursos.delete_one({'_id': ObjectId(curso_id)}).delete_count)


# Consultar curso por ID proyección para obtener la información del curso que vamos a guardar en nuestra coleccion de carreras
def consultar_curso_por_id_proyeccion(id_curso, proyeccion=None):
    return db.cursos.find_one({'_id': ObjectId(id_curso)}, proyeccion)


def consultar_curso_por_nombre(nombre):
    return dumps(db.cursos.find({'$text': {'$search': nombre}}))

