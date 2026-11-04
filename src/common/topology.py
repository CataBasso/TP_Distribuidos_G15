from common.middleware import partition

#Nombres de colas y exchanges, y convenciones de mensajes, compartidos por las etapas
#LengthMapper, AvgReducer, Q5Filter y el Gateway
ABORT_EX = "abort_ex"   #Gateway -> todas las etapas con estado por cliente
TOK_EX = "tok_ex"   #AbstractCleaner -> {WordMapper, LengthMapper}
LM_IN = "lm_in" #Cola de LengthMapper
LM_CTRL = "lm_ctrl" #EOF entre las replicas de LengthMapper
WM_IN = "wm_in"  #Cola de WordMapper
WM_CTRL = "wm_ctrl" #EOF entre las replicas de WordMapper
WORDS_EX = "words_ex"   #WordMapper -> WordReducer (hash por palabra)
WORDS_Q = "words_q" #Una cola por particion del WordReducer
TOP_IN = "top_in"   #Cola del TopMerger: tops locales de los WordReducer
AVG_PEX = "avg_pex" #LengthMapper -> AvgReducer (hash por cliente)
AVG_IN = "avg_in"   #una cola por particion del AvgReducer
AVG_EX = "avg_ex"   #AvgReducer -> todos los Q5Filter (avg_ready)
AVG_READY_Q = "avg_ready_q" #Cola de cada Q5Filter[k]
Q5_PEX = "q5_pex"   #LengthMapper -> Q5Filter (hash por K)
Q5_IN = "q5_in" #Una cola por particion de Q5
RESULTS_EX = "results_ex"   #Todos los productores de resultados -> Gateway (clave=gw_id)
RESULTS_Q = "results_q" #Cola results_q.<g>: la consume el Gateway g

EXCHANGE_KINDS = {TOK_EX: "fanout", LM_CTRL: "fanout", WM_CTRL: "fanout", AVG_EX: "fanout", ABORT_EX: "fanout"}

#Nombres de etapa
STAGE_ABSTRACT_CLEANER = "abstract_cleaner"
STAGE_LENGTH_MAPPER = "length_mapper"
STAGE_WORD_MAPPER = "word_mapper"
STAGE_AVG_REDUCER = "avg_reducer"
STAGE_Q5_FILTER = "q5_filter"

#Etiquetas de peroodo que pone el YearFilter y conserva el AbstractCleaner
PERIOD_UP_TO_2020 = "<=2020"
PERIOD_FROM_2021 = ">=2021"

#Tamano del top de Q4
TOP_N = 20

#MAx cant de filas de cada mensaje 'result'
RESULT_CHUNK_ROWS = 500

#avg_in.3, q5_in.0, results_q.1
def queue_of(prefix: str, index) -> str:
    return f"{prefix}.{index}"


def client_key(client_id) -> str:
    return f"{int(client_id[0])}.{int(client_id[1])}"

#Clave de results_ex: el gw_id del cliente.
def results_key(client_id) -> str:
    return str(client_id[0])

#EOF que el Gateway espera por consulta para dar por completo a un cliente: 1 para Q1, Q2 y Q4; P para Q3; K para Q5.
def expected_result_eofs(word_partitions: int, q5_partitions: int) -> dict:
    return {1: 1, 2: 1, 3: word_partitions, 4: 1, 5: q5_partitions}


__all__ = [n for n in dir() if n.isupper()] + [
    "partition", "queue_of", "client_key", "results_key", "expected_result_eofs",
]
