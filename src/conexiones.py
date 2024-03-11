import re
import pyodbc
from typing import Dict
from pyspark.sql import SparkSession, DataFrame, types as t

from src.utils.spark_session import ConfigSpark
from src.utils.key_vault import KeyVault


class Conexiones:

    token_library = KeyVault()
    get_secret = token_library.get_secret
    spark: SparkSession = ConfigSpark().get_spark()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    def __init__(self) -> None:
        self.datalake = self.spark.sparkContext.environment.get("storageaccount", "get")
        self.key_vault = self.spark.sparkContext.environment.get("keyvault", "get")
        self.odbc_driver = pyodbc.drivers()[0]

        self.conexion_sql_simem = (
            self.get_secret(self.key_vault, "cndbsSimemConfiguracion", "lskeyvault")
            or ""
        )
        self.conexion_sql_xm = (
            self.get_secret(self.key_vault, "cndbsdatalakesqlConfigXM", "lskeyvault")
            or ""
        )

        self.url_simem, self.conexion_simem, self.properties_simem = (
            self.create_jdbc_url(self.conexion_sql_simem)
        )

        self.url_xm, self.conexion_xm, self.properties_xm = self.create_jdbc_url(
            self.conexion_sql_xm
        )

        self.leer_todas_las_tablas()

    def leer_todas_las_tablas(self) -> None:
        """
        Lee y asigna múltiples tablas de bases de datos a atributos de instancia, usando JDBC.

        Este método inicializa atributos con DataFrames de Spark, leyendo datos de tablas específicas
        de bases de datos. Las tablas leídas incluyen configuraciones, índices, columnas de origen y destino,
        duraciones, granularidades, entre otras, pertenecientes a diferentes esquemas y fuentes de datos
        ('simem' o 'xm').

        Los DataFrames resultantes se utilizan para operaciones de procesamiento de datos posteriores,
        accesibles como atributos de la instancia.

        No toma parámetros ni retorna valores. La conexión a la base de datos y la lectura se realizan
        a través del método `read_table`, con configuraciones de conexión JDBC específicas.
        """

        self.generacion_archivos = self.read_table(
            schema="Configuracion", table="GeneracionArchivos"
        )
        self.configuracion_extracciones_xm = self.read_table(
            schema="Configuracion", table="Extracciones"
        )
        self.index_datalake_xm = self.read_table(table="IndexDatalake", origen="xm")
        self.columnas_origen = self.read_table(
            table="ColumnasOrigen", schema="Configuracion"
        )
        self.columnas_destino = self.read_table(
            table="ColumnasDestino", schema="Configuracion"
        )
        self.duracion_iso = self.read_table(table="DuracionISO", schema="Configuracion")
        self.granularidad_dataset = self.read_table(
            table="Granularidad", schema="Configuracion"
        )
        self.df_configuracion_estandarizacion_registros = self.read_table(
            table="EstandarizacionRegistros", schema="Configuracion"
        )
        self.columnas_origen_xm = self.read_table(
            table="ConfiguracionColumnasOrigen", origen="xm"
        )
        self.columnas_destino_xm = self.read_table(
            table="ConfiguracionColumnasDestino", origen="xm"
        )
        self.configuracion_transformaciones_xm = self.read_table(
            table="ConfiguracionExtracciones", origen="xm"
        )
        self.periodicidad = self.read_table(
            table="Periodicidad", schema="Configuracion"
        )
        self.configuracion_ejecuciones = self.read_table(
            table="Ejecucion", schema="Configuracion"
        )
        self.tiempo = self.read_table(table="Tiempo", origen="xm")
        self.dato_archivo_simem = self.read_table(table="Archivo", schema="Dato")
        self.df_publicacion_regulatoria = self.read_table(
            table="PublicacionRegulatoria", schema="Configuracion"
        )
        self.df_clasificacion_regulatoria = self.read_table(
            table="ClasificacionRegulatoria", schema="Configuracion"
        )

    def create_jdbc_url(self, conn_parameters: str) -> tuple[str, str, Dict[str, str]]:
        """
        Crea una URL JDBC y un diccionario de propiedades a partir de los parametros de conexion.

        Este método analiza una cadena de conexión str proporcionada para extraer los componentes
        necesarios (servidor, base de datos, usuario y contraseña) y construir una URL JDBC
        y un diccionario de propiedades que se pueden utilizar para establecer una conexión JDBC.

        Parameters:
            - conn_parameters (str): Parametros de conexion que incluye el servidor, base de datos,
                            identificador de usuario y contraseña. La cadena debe seguir el formato:
                            "Server=XXX;Database=YYY;User Id=ZZZ;Password=AAAA;"

        Returns:
            - tuple[str, str, Dict[str, str]]: Una tupla que contiene la URL JDBC como primer elemento
                                        y un diccionario de propiedades como segundo elemento.
                                        Las propiedades incluyen 'user', 'password' y 'driver'.

        Raises:
            - ValueError: Si los parametros de conexion no tienen el formato esperado o falta
                        alguna de las partes requeridas (servidor, base de datos, usuario, contraseña).

        Example:
            >>> create_jdbc_url("Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;")
            ("jdbc:sqlserver://myServerAddress:1433;databaseName=myDataBase", {"user": "myUsername", "password": "myPassword", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"})
        """

        pattern = re.compile(
            r"Server=(?P<server>[^;]+);"
            r"Database=(?P<database>[^;]+);"
            r"User Id=(?P<user>[^;]+);"
            r"Password=(?P<password>[^;]+);"
        )

        match = pattern.search(conn_parameters)
        if match:
            components = match.groupdict()
            jdbc_url = f"jdbc:sqlserver://{components['server']}:1433;databaseName={components['database']}"
            odbc_url = f"DRIVER={self.odbc_driver};SERVER={components['server']};DATABASE={components['database']};UID={components['user']};PWD={components['password']}"
            properties = {
                "user": components["user"].strip(),
                "password": components["password"],
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            }
        else:  # pragma: no cover
            raise ValueError

        return jdbc_url, odbc_url, properties

    def read_table(
        self, table: str, schema: str = "dbo", origen: str = "simem"
    ) -> DataFrame:
        """
        Lee una tabla de una base de datos SQL utilizando una conexión JDBC,
        basada en la configuración de conexión especificada para el origen
        'simem' o 'xm'. Devuelve un DataFrame de Spark con los datos de la tabla.

        La conexión JDBC y las propiedades se determinan según el origen
        especificado. Si el origen es 'xm', se utilizan las configuraciones
        de 'self.url_xm' y 'self.properties_xm'; de lo contrario, se utilizan
        las configuraciones de 'self.url_simem' y 'self.properties_simem'.

        Parameters:
            - table (str): Nombre de la tabla a leer.
            - schema (str, optional): Esquema de la base de datos donde se encuentra la tabla.
                                    Por defecto es 'dbo'.
            - origen (str, optional): Indica el conjunto de configuraciones de conexión a
                                    usar. Puede ser 'simem' o 'xm'. Por defecto es 'simem'.

        Returns:
            - DataFrame: Un DataFrame de Spark que contiene los datos leídos de la tabla especificada.

        Raises:
            - ValueError: Si los parámetros proporcionados no son válidos o si
                        ocurre un error durante la conexión o la lectura de la base de datos.

        Example:
            >>> df = conexiones.read_table(table="nombre_tabla", schema="esquema_tabla", origen="simem")
        """
        if origen == "xm":
            url = self.url_xm
            properties = self.properties_xm
        else:  # origen == "simem"
            url = self.url_simem
            properties = self.properties_simem
        df_table = self.spark.read.jdbc(
            url, table=f"[{schema}].[{table}]", properties=properties
        )
        return df_table

    def extracciones_archivos(self, nombre_archivo_destino: str) -> DataFrame:
        """
        Ejecuta una consulta SQL para extraer datos relacionados con extracciones
        específicas basadas en el nombre del archivo destino.

        Parameters:
            - nombre_archivo_destino (str): Nombre del archivo destino para filtrar las
                                            extracciones relevantes.

        Returns:
            - DataFrame: Un DataFrame de Spark que contiene las columnas IdExtraccion,
                        FechaDeltaInicial, y FechaDeltaFinal para las extracciones que coinciden
                        con el criterio especificado.

        La consulta SQL se ejecuta dentro del contexto de Spark SQL, combinando
        información de las vistas temporales 'Extracciones' y 'GeneracionArchivo'.
        """
        self.configuracion_extracciones_xm.createOrReplaceTempView("Extracciones")
        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivo")

        sql = f"""SELECT Extracciones.IdExtraccion,Extracciones.FechaDeltaInicial,Extracciones.FechaDeltaFinal FROM Extracciones
                LEFT JOIN GeneracionArchivo ON Extracciones.IdConfiguracionGeneracionArchivos=GeneracionArchivo.IdConfiguracionGeneracionArchivos
                WHERE GeneracionArchivo.NombreArchivoDestino='{nombre_archivo_destino}'"""

        return self.spark.sql(sql)

    def info_como_cotejo(self, tema: str, nombre_archivo_destino: str) -> DataFrame:
        """
        Ejecuta una consulta SQL para obtener información relevante para el cotejo de archivos.

        Parameters:
            - tema (str): El tema asociado a la configuración de generación de archivos.
            - nombre_archivo_destino (str): El nombre del archivo destino para el cual se realiza el cotejo.

        Returns:
            - DataFrame: Un DataFrame de Spark que contiene los resultados de la consulta SQL, incluyendo
                        la configuración de generación de archivos, periodicidad, y las columnas destino
                        específicas para el tema y nombre de archivo proporcionados.

        La consulta SQL agrupa datos de varias tablas relacionadas a la generación de archivos,
        incluyendo periodicidad y columnas destino, filtrando por el tema y nombre del archivo
        destino especificados.
        """

        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")
        self.periodicidad.createOrReplaceTempView("Periodicidad")
        self.columnas_destino.createOrReplaceTempView("ColumnasDestino")

        sql_info = f"""SELECT IdConfiguracionGeneracionArchivos
                    ,A.Tema
                    ,A.NombreArchivoDestino
                    ,A.SelectXM
                    ,A.ValorDeltaInicial
                    ,A.ValorDeltaFinal
                    ,B.Periodicidad
                    ,A.IntervaloPeriodicidad
                    ,C.NombreColumnaDestino
                    ,A.DatoObligatorio
                    ,C2.NombreColumnaDestino AS NombreColumnaVersion
                    FROM GeneracionArchivos AS A
                    LEFT JOIN Periodicidad AS B ON A.IdPeriodicidad=B.IdPeriodicidad  
                    LEFT JOIN ColumnasDestino AS C ON A.IdColumnaDestino=C.IdColumnaDestino
                    LEFT JOIN ColumnasDestino AS C2 ON A.IdColumnaVersion=C2.IdColumnaDestino
                    WHERE A.Tema='{tema}' AND A.NombreArchivoDestino='{nombre_archivo_destino}'"""

        return self.spark.sql(sql_info)

    def duracion_iso_registro(self, tema: str, nombre_archivo_destino: str) -> str:
        """
        Obtiene la duración en formato ISO 8601 para un registro específico basado en su tema y archivo destino.

        Parameters:
            - tema (str): El tema relacionado con el registro de interés.
            - nombre_archivo_destino (str): El nombre del archivo destino asociado al registro.

        Returns:
            str: Una cadena que representa la duración del registro en formato ISO 8601.

        Este método ejecuta una consulta SQL sobre las vistas temporales 'GeneracionArchivos' y
        'DuracionISO' para encontrar la duración ISO 8601 correspondiente al tema y nombre de archivo
        proporcionados. Asume que la consulta devuelve al menos un resultado y extrae el código ISO
        8601 del primer registro.
        """

        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")
        self.duracion_iso.createOrReplaceTempView("DuracionISO")

        sql_duracion = f"""SELECT B.CodigoISO8601 FROM GeneracionArchivos A
                        Left join DuracionISO B ON A.IdDuracionISO=B.IdDuracionISO
                        WHERE A.Tema='{tema}' AND A.NombreArchivoDestino='{nombre_archivo_destino}' """

        df = self.spark.sql(sql_duracion)
        duracion = df.collect()[0].codigoISO8601
        return duracion

    def rutas_archivos_xm(
        self, tema: str, nombre_archivo_destino: str, pdf: bool = False
    ) -> DataFrame:
        """
        Obtiene las rutas y detalles de extracciones XM asociadas a un tema y archivo destino específicos.

        Parameters:
            - tema (str): El tema bajo el cual se clasifican los archivos y extracciones.
            - nombre_archivo_destino (str): El nombre del archivo de destino para el cual se buscan rutas.

        Returns:
            - DataFrame: Un DataFrame de Spark que contiene las rutas y detalles de las extracciones XM,
                        incluyendo el ID de extracción, directorio, nombre de extracción, ID de configuración de
                        generación de archivos, y las fechas de inicio y fin del intervalo de extracción.

        Este método prepara y ejecuta una consulta SQL compleja que cruza información entre
        las vistas temporales 'GeneracionArchivos', 'ConfiguracionExtracciones', y 'IndexDataLake'
        para identificar las rutas y detalles relevantes de las extracciones XM que cumplen con
        los criterios especificados por 'tema' y 'nombre_archivo_destino'.
        """

        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")
        self.configuracion_extracciones_xm.createOrReplaceTempView(
            "ConfiguracionExtracciones"
        )
        self.index_datalake_xm.createOrReplaceTempView("IndexDataLake")

        condition = (
            "WHERE make_timestamp(Ano,Mes,Dia,Hora,0,0) BETWEEN FechaDeltaInicial AND FechaDeltaFinal"
            if pdf
            else " WHERE DeltaLake = 1"
        )

        sql_rutas_archivos_xm = f"""
            WITH ArchivoGeneracion AS
            (
                SELECT IdConfiguracionGeneracionArchivos FROM GeneracionArchivos
                WHERE Tema='{tema}' AND NombreArchivoDestino='{nombre_archivo_destino}'
            )
            ,
            ExtraccionesXM AS
            (
            SELECT configuracionextracciones.IdExtraccion,
                    Proyecto,
                    Tema,
                    NombreExtraccion,
                    FechaDeltaInicial,
                    FechaDeltaFinal,
                    ArchivoGeneracion.IdConfiguracionGeneracionArchivos FROM  ConfiguracionExtracciones 
            INNER JOIN ArchivoGeneracion ON ConfiguracionExtracciones.IdConfiguracionGeneracionArchivos=ArchivoGeneracion.IdConfiguracionGeneracionArchivos
            )

            SELECT DISTINCT ExtraccionesXM.IdExtraccion,Directorio,
                    ExtraccionesXM.NombreExtraccion,
                    ExtraccionesXM.IdConfiguracionGeneracionArchivos,
                    ExtraccionesXM.FechaDeltaInicial,
                    ExtraccionesXM.FechaDeltaFinal
                FROM IndexDataLake
            INNER JOIN ExtraccionesXM ON IndexDataLake.Proyecto=ExtraccionesXM.Proyecto
                                        AND IndexDataLake.Tema=ExtraccionesXM.Tema 
                                        AND IndexDataLake.NombreExtraccion=ExtraccionesXM.NombreExtraccion  
            {condition}
            """

        return self.spark.sql(sql_rutas_archivos_xm)

    def data_estandarizacion(self, tema: str, nombre_archivo_destino: str) -> DataFrame:
        """
        Obtiene la configuración de estandarización de columnas para un tema y archivo destino específicos.

        Parameters:
            - tema (str): El tema asociado a la configuración de estandarización.
            - nombre_archivo_destino (str): El nombre del archivo destino de interés.

        Returns:
            DataFrame: Un DataFrame que contiene la configuración de estandarización para las columnas de origen,
                        incluyendo el nombre de la columna de origen, el nombre de la columna de destino y el tipo de dato.

        Este método ejecuta una consulta SQL que une las tablas 'GeneracionArchivos', 'ColumnasOrigen',
        y 'ColumnasDestino' para obtener la configuración necesaria para la estandarización de los datos,
        filtrando por el tema y el nombre del archivo destino especificado.
        """
        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")
        self.columnas_origen.createOrReplaceTempView("ColumnasOrigen")

        sql_estandarizacion_columnas = f"""
            SELECT NombreColumnaOrigen,
                NombreColumnaDestino,
                TipoDato FROM ColumnasOrigen
                INNER JOIN GeneracionArchivos ON GeneracionArchivos.IdConfiguracionGeneracionArchivos=ColumnasOrigen.IdConfiguracionGeneracionArchivos
                INNER JOIN ColumnasDestino ON ColumnasDestino.IdColumnaDestino=ColumnasOrigen.IdColumnaDestino
            WHERE GeneracionArchivos.Tema='{tema}' AND NombreArchivoDestino = '{nombre_archivo_destino}'
        """

        return self.spark.sql(sql_estandarizacion_columnas)

    def aplicar_sql_simem(self, query: str, values: tuple[str, ...]) -> None:
        """
        Ejecuta una consulta SQL en la base de datos SIMEM utilizando los parámetros especificados.

        Parameters:
            - query (str): La consulta SQL a ejecutar.
            - values (tuple): Una tupla que contiene los valores a usar en la consulta SQL, correspondientes a cualquier marcador de posición en la consulta.

        Returns:
            None: Este método no devuelve un valor. Realiza la ejecución de la consulta y aplica los cambios en la base de datos.

        Este método establece una conexión con la base de datos SIMEM, crea un cursor, ejecuta la consulta SQL proporcionada con los valores dados, confirma los cambios y luego cierra el cursor y la conexión.
        """

        conn = pyodbc.connect(self.conexion_simem)
        cursor = conn.cursor()
        cursor.execute(query, values)
        cursor.commit()
        cursor.close()

    def actualizar_deltas_xm(self, df_xm: DataFrame) -> None:
        """
        Ejecuta un procedimiento almacenado para actualizar las deltas de la tabla Configuracion.Extracciones en la base de datos.

        Parameters:
            - df_xm (DataFrame): Un DataFrame de Spark que contiene los ID de las extracciones XM a actualizar.

        Este método recorre cada fila del DataFrame df_xm, extrae el IdExtraccio` y llama al método
        aplicar_sql_simem` con una consulta SQL para ejecutar el procedimiento almacenado
        sp_ActualizarDeltasXM` en la base de datos SIMEM, pasando el IdExtraccion como parámetro.
        """

        for data in df_xm.collect():
            query = "EXEC [Configuracion].[sp_ActualizarDeltasXM] ?"
            values = data.IdExtraccion
            self.aplicar_sql_simem(query=query, values=values)

    def actualizar_deltas_generacion_archivo(self, id_generacion_archivo: str) -> None:
        """
        Actualiza los deltas para una generación de archivo específica ejecutando un procedimiento almacenado en la base de datos.

        Parameters:
            - id_generacion_archivo (str): Identificador de la generación de archivo cuyos deltas necesitan ser actualizados.

        Este método ejecuta el procedimiento almacenado 'sp_ActualizarDeltasGeneracionArchivo' pasando el ID de la generación del archivo.
        Se utiliza para mantener actualizada la información de deltas de generación de archivo en la base de datos.
        """
        query = "EXEC [Configuracion].[sp_ActualizarDeltasGeneracionArchivo] ?"
        values = tuple(id_generacion_archivo)
        self.aplicar_sql_simem(query=query, values=values)

    def dataframe_calidad_datos(
        self, tema: str, nombre_archivo_destino: str
    ) -> DataFrame:
        """
        Consulta criterios de calidad de datos para un tema y archivo destino específicos, retornando un DataFrame con los resultados.

        Parameters:
            - tema (str): Tema asociado a los criterios de calidad de datos a consultar.
            - nombre_archivo_destino (str): Nombre del archivo destino asociado a los criterios de calidad de datos.

        Returns:
            DataFrame: Un DataFrame de Spark que contiene los criterios de calidad de datos, fórmulas de calificación,
            descripciones de solución, fechas de vigencia, columnas afectadas, códigos y descripciones de error, y
            granularidad y número de regla de actualidad.

        Este método ejecuta el procedimiento almacenado 'sp_Calidad_ColumnasCriterios' en la base de datos SIMEM,
        convierte los resultados en una lista de tuplas, y finalmente crea un DataFrame de Spark utilizando un esquema
        definido para representar la información de calidad de datos.
        """
        cnxn = pyodbc.connect(self.conexion_simem)
        cursor = cnxn.cursor()
        cursor.execute(
            "Exec [CalidadDatos].[sp_Calidad_ColumnasCriterios] ?,?",
            (tema, nombre_archivo_destino),
        )
        data = cursor.fetchall()
        data = [tuple(row) for row in data]

        schema = t.StructType(
            [
                t.StructField("CriterioCalidad", t.StringType(), True),
                t.StructField("FormulaCalificacion", t.StringType(), True),
                t.StructField("DescSolucion", t.StringType(), True),
                t.StructField("FechaVigInicial", t.TimestampType(), True),
                t.StructField("FechaVigFinal", t.TimestampType(), True),
                t.StructField("Columnas", t.StringType(), True),
                t.StructField("CodError", t.StringType(), True),
                t.StructField("DescError", t.StringType(), True),
                t.StructField("GranularidadCalidad", t.StringType(), True),
                t.StructField("NumeroReglaActualidad", t.StringType(), True),
            ]
        )
        cursor.close()

        return self.spark.createDataFrame(data=data, schema=schema)

    def columnas_destino_extraccion(
        self, tema: str, nombre_archivo_destino: str
    ) -> set:
        """
        Obtiene un conjunto de nombres de columnas destino asociadas a un tema y archivo destino específicos.

        Parameters:
            - tema (str): Tema asociado a la extracción de datos.
            - nombre_archivo_destino (str): Nombre del archivo destino de interés.

        Returns:
            set: Un conjunto de cadenas que representan los nombres únicos de las columnas destino.

        Este método consulta las configuraciones de columnas origen y destino, filtrando por tema y
        nombre de archivo destino. Utiliza una vista temporal para realizar las consultas necesarias
        en Spark SQL y recopila los resultados en un conjunto para eliminar duplicados, proporcionando
        así una lista única de nombres de columnas destino relevantes para la extracción de datos.
        """

        self.columnas_destino.createOrReplaceTempView("ConfiguracionColumnasDestino")
        self.columnas_origen.createOrReplaceTempView("ConfiguracionColumnasOrigen")
        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")

        sql_valores_estandarizados = f"""
            SELECT  B.NombreColumnaDestino
                FROM ConfiguracionColumnasOrigen AS A 
                JOIN ConfiguracionColumnasDestino AS B 
                ON A.IdColumnaDestino=B.IdColumnaDestino
                JOIN GeneracionArchivos AS C ON A.IdConfiguracionGeneracionArchivos=C.IdConfiguracionGeneracionArchivos
            WHERE  C.Tema='{tema}' AND C.NombreArchivoDestino='{nombre_archivo_destino}'
        """

        df_columnas_destino = self.spark.sql(sql_valores_estandarizados)

        lista_valores = (
            df_columnas_destino.select("NombreColumnaDestino")
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        return set(lista_valores)

    def dataframe_estandarizacion_filas(self) -> DataFrame:
        """
        Consulta y retorna un DataFrame con la configuración de estandarización de las filas.

        Este método prepara y ejecuta una consulta SQL utilizando las vistas temporales 'ConfiguracionEstandarizacionRegistros'
        y 'ConfiguracionColumnasDestino' para seleccionar los nombres de las columnas destino y sus respectivos valores objetivo.
        Es utilizado para obtener las directrices de estandarización aplicables a las columnas de datos, facilitando procesos de limpieza
        y estandarización de datos según configuraciones predefinidas.

        Returns:
            DataFrame: Un DataFrame que contiene las configuraciones de estandarización para las columnas destino, incluyendo
                        los nombres de las columnas y sus valores objetivo correspondientes.
        """

        self.df_configuracion_estandarizacion_registros.createOrReplaceTempView(
            "ConfiguracionEstandarizacionRegistros"
        )
        self.columnas_destino.createOrReplaceTempView("ConfiguracionColumnasDestino")

        sql_registros = """
            SELECT NombreColumnaDestino,
                ValorObjetivo 
                FROM ConfiguracionEstandarizacionRegistros A
                JOIN ConfiguracionColumnasDestino B ON A.IdColumnaDestino=B.IdColumnaDestino
        """

        return self.spark.sql(sql_registros)

    def select_transformacion(self, nombre_extraccion: str) -> DataFrame:
        """
        Genera un DataFrame con nombres de columnas destino y tokens de reemplazo basados en una extracción específica.

        Parameters:
            - nombre_extraccion (str): Nombre de la extracción para la cual se buscan las transformaciones.

        Retorna un DataFrame de Spark que mapea cada columna destino con un token de reemplazo específico,
        diseñado para facilitar la sustitución de valores durante el proceso de transformación de datos.
        Este mapeo es crucial para automatizar y estandarizar la manipulación de datos en el proceso de extracción.
        """

        self.configuracion_transformaciones_xm.createOrReplaceTempView(
            "ConfiguracionExtracciones"
        )
        self.columnas_origen_xm.createOrReplaceTempView("ConfiguracionColumnasOrigen")
        self.columnas_destino_xm.createOrReplaceTempView("ConfiguracionColumnasDestino")

        sql_nombres_reemplazo = f"""
            SELECT B.NombreColumnaDestino
                ,concat('<<Columna_',CAST(NumeracionColumna AS string),'_',C.NombreExtraccion,'>>') AS TokenColumna
                FROM ConfiguracionColumnasOrigen AS A
                LEFT JOIN ConfiguracionColumnasDestino AS B ON A.IdColumnaDestino=B.IdColumnaDestino
                LEFT JOIN ConfiguracionExtracciones AS C ON A.IdExtraccion=C.IdExtraccion
            WHERE C.NombreExtraccion='{nombre_extraccion}'
        """

        return self.spark.sql(sql_nombres_reemplazo)

    def dataframe_regulatorios(self):
        """
        Construye un DataFrame con la configuración regulatoria para las generaciones de archivos.

        Este método une las configuraciones de publicación y clasificación regulatoria con las generaciones de archivos, filtrando
        por aquellos registros que tienen indicador regulatorio activo. El resultado es un DataFrame que incluye detalles regulatorios
        como días, meses, días de la semana, indicadores de día hábil y otras clasificaciones regulatorias aplicables.

        Returns:
            DataFrame: Un DataFrame que contiene información detallada regulatoria asociada a las configuraciones de generación de archivos.
        """

        self.df_publicacion_regulatoria.createOrReplaceTempView(
            "ConfiguracionPublicacionRegulatoria"
        )
        self.df_clasificacion_regulatoria.createOrReplaceTempView(
            "ConfiguracionClasificacionRegulatoria"
        )
        self.generacion_archivos.createOrReplaceTempView("GeneracionArchivos")
        sql_registros = """
            SELECT
                A.IdConfiguracionGeneracionArchivos,
                A.IndRegulatorio,
                B.Dia,
                B.Mes,
                B.DiaSemana,
                B.IndDiaHabil,
                c.*
            FROM GeneracionArchivos A
            JOIN ConfiguracionPublicacionRegulatoria B
                on A.IdConfiguracionGeneracionArchivos = B.IdGeneracionArchivo
            JOIN ConfiguracionClasificacionRegulatoria C
                on A.IdConfiguracionClasificacionRegulatoria = C.IdConfiguracionClasificacionRegulatoria
            WHERE IndRegulatorio = 1
        """

        return self.spark.sql(sql_registros)
