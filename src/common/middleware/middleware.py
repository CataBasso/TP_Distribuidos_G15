"""
¿Qué clase usar en cada caso?

    Cola de trabajo (MessageMiddlewareQueue)
        Varias réplicas consumen la misma cola y cada mensaje lo procesa UNA sola.
        Sirve para repartir carga: year_in, q1_in, q2_in, norm_in, img_in, top_in, out_q.<g>

    Exchange fanout (MessageMiddlewareExchange, exchange_type='fanout')
        Cada mensaje llega a TODAS las colas ligadas al exchange; la routing key se ignora.
        Sirve para difundir: tok_ex, avg_ex, los *_ctrl y abort_ex.

    Exchange direct (MessageMiddlewareExchange, exchange_type='direct')
        La routing key del mensaje decide a qué cola(s) llega. Sirve para particionar:
        words_ex, avg_pex y q5_pex (clave = partición, ver partition.py) y
        results_ex (clave = gw_id).

    Cola con nombre o anónima (parámetro queue_name del exchange)
        - Con nombre: cola durable, compartida por quien use ese nombre (o la cola de una
        partición, ej. words_q.3). Se crea y se liga en el __init__, así el productor
        puede enviar antes de que el consumidor haya arrancado sin perder mensajes.
        - Anónima (queue_name=None): cola exclusiva de este consumidor, se borra al cerrar.
        Sirve para ctrl y abort, donde cada réplica necesita su propia copia del mensaje.

    Conexión compartida (MessageMiddlewareConnection)
        Un proceso que escucha varias fuentes a la vez (por ejemplo datos + ctrl + abort)
        Crea UNA conexión, se la pasa a cada endpoint (connection=...), registra cada
        consumidor con consume() y atiende todo con un único run(). Para cortar, por
        ejemplo ante SIGTERM, se llama a stop(), que es seguro desde un handler de señal
        o desde otro hilo.

"""

from abc import ABC, abstractmethod


class MessageMiddlewareMessageError(Exception):
    """Error interno que no puede resolverse (mensaje rechazado, canal cerrado por el broker, etc.)."""


class MessageMiddlewareDisconnectedError(Exception):
    """Se perdió la conexión con el broker, o no se pudo conectar tras los reintentos."""


class MessageMiddlewareCloseError(Exception):
    """Falló el cierre de la conexión o del canal."""


class MessageMiddlewareDeleteError(Exception):
    """Falló la eliminación de la cola o del exchange."""


class MessageMiddlewareConnection(ABC):
    """
    Conexión con el broker. Puede compartirse entre varios MessageMiddleware de un mismo
    proceso (parámetro connection=), de modo que un único loop atienda todas sus colas.

    No es thread-safe: se usa desde un solo hilo. La única operación que puede llamarse
    desde otro hilo o desde un handler de señales es stop().
    """

    # Atiende a todos los consumidores registrados con consume(). Bloquea hasta que alguien llame a stop().
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def run(self):
        pass

    # Hace que run() termine. Es seguro llamarlo desde un callback, desde otro hilo o desde un handler de SIGTERM. 
    # Si se pierde la conexión eleva MessageMiddlewareDisconnectedError.
    @abstractmethod
    def stop(self):
        pass

    # Cierra la conexión con el broker.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    @abstractmethod
    def close(self):
        pass


class MessageMiddleware(ABC):
    """
    Punto de acceso a una cola o a un exchange: permite enviar mensajes y consumirlos.
    Los mensajes viajan como bytes: serializar y deserializar es tarea de common.protocol.
    """

    # Registra el consumidor SIN bloquear. Después hay que atender los mensajes con
    # run() de la conexión (cuando se comparte una conexión entre varios endpoints).
    # on_message_callback tiene los mismos parámetros que en start_consuming.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def consume(self, on_message_callback):
        pass

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje. Bloquea hasta que
    # se llame a stop_consuming(). Equivale a consume() seguido de run().
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    #       Puede guardarse y llamarse más tarde (ack diferido).
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    #        Por defecto el mensaje vuelve a la cola; nack(requeue=False) lo descarta.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    @abstractmethod
    def stop_consuming(self):
        pass

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # En los exchange direct, la implementación acepta además routing_key=... para elegir
    # la clave de ese mensaje (por defecto usa la primera de las routing keys).
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def send(self, message):
        pass

    # Elimina la cola (y el exchange, si corresponde) del broker. Se usa para limpiar en
    # los tests; el sistema trabaja con colas estáticas y normalmente no borra nada.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
    @abstractmethod
    def delete(self):
        pass

    # Se desconecta de la cola o exchange al que estaba conectado. Si la conexión es
    # compartida, no la cierra: la cierra quien la creó con MessageMiddlewareConnection.close().
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    @abstractmethod
    def close(self):
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    """
    Exchange del broker, con una o más routing keys.

    host          - Dirección del broker.
    exchange_name - Nombre del exchange.
    routing_keys  - Claves con las que se liga la cola de este consumidor. Si el exchange es fanout se ignoran.
    exchange_type - 'direct' (por defecto): la clave decide la cola de destino.
                    'fanout': el mensaje llega a todas las colas ligadas.
    queue_name    - Cola con nombre (durable, compartida o de una partición), creada y
                    ligada al construir. None: cola anónima y exclusiva de este
                    consumidor, creada al consumir.
    durable       - Si el exchange, la cola con nombre y los mensajes sobreviven a un
                    reinicio del broker.
    connection    - Conexión compartida con otros endpoints del proceso. None: crea la suya.
    """

    @abstractmethod
    def __init__(self, host, exchange_name, routing_keys, exchange_type='direct',
                 queue_name=None, durable=True, connection=None):
        pass


class MessageMiddlewareQueue(MessageMiddleware):
    """
    Cola de trabajo: varias réplicas consumen de la misma cola y cada mensaje lo procesa
    una sola de ellas (competing consumers).

    host       - Dirección del broker.
    queue_name - Nombre de la cola.
    connection - Conexión compartida con otros endpoints del proceso. None: crea la suya.
    durable    - Si la cola y los mensajes sobreviven a un reinicio del broker.
    """

    @abstractmethod
    def __init__(self, host, queue_name, connection=None, durable=True):
        pass