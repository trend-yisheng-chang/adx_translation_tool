from .query_structure.template import SparkSQLTemplate, KustoTemplate
from .query_structure.similar_query import SimilarQuery
from . import constant
import openai
import os
import re
from pyspark.sql import SparkSession


class Translation():
    def __init__(self, adx_functions_path, adx_tables_path, openai_api_key=None):
        self.spark = SparkSession.builder.getOrCreate()
        self.kusto_template = KustoTemplate(self._read_function_csv(
            adx_functions_path), self._read_table_csv(adx_tables_path))
        self.similar_query = SimilarQuery(constant.GROUND_TRUTH_JSON_PATH)
        openai.base_url = constant.OPEN_AI_BASE_URL
        openai.api_key = openai_api_key or os.getenv('OPEN_AI_API_KEY')
        self.output_sparsql_pattern = r'<sparksql>([\s\S]*?)<\/sparksql>'

    def translate(self, kusto_query):
        extracted_kusto_query = self.kusto_template.to_template(kusto_query)
        top_3_similar_queries = self.similar_query.get_top_k_similar_queries(
            extracted_kusto_query, k=3)
        llm_prompt = self._generate_llm_prompt(
            kusto_query, top_3_similar_queries)
        spark_sql = None
        while not (spark_sql and self._is_valid_spark_sql(spark_sql)):
            response = self._ask_llm(llm_prompt)
            matches = re.findall(self.output_sparsql_pattern, response)
            if matches:
                spark_sql = matches[0]
        cleaned_spark_sql = SparkSQLTemplate.clean(spark_sql)
        formatted_spark_sql = SparkSQLTemplate.format(cleaned_spark_sql)
        return formatted_spark_sql

    def _read_function_csv(self, adx_functions_path):
        functions_df = self.spark.read.option("escape", "\"").option(
            "multiLine", True).csv(adx_functions_path, header=True).collect()
        return [f['Name'] for f in functions_df]

    def _read_table_csv(self, adx_tables_path):
        tables_df = self.spark.read.option("escape", "\"").option(
            "multiLine", True).csv(adx_tables_path, header=True).collect()
        return [t['TableName'] for t in tables_df]

    def _is_valid_spark_sql(self, spark_sql_query):
        return True

    def _ask_llm(self, llm_prompt):
        try:
            response = openai.chat.completions.create(
                model='gpt-4o',
                messages=[
                    {
                        "role": "system",
                        "content": self._generate_llm_guide()
                    },
                    {
                        "role": "user",
                        "content": llm_prompt
                    }
                ],
            )
            response_text = response.choices[0].message.content
            return response_text
        except openai.OpenAIError as e:
            raise Exception('OpenAI API returned an API Error: {}'.format(e))

    def _generate_llm_guide(self):
        return """You are a Kusto to Spark SQL translator, I will give you several pairs of (Kusto, Spark SQL) as references where you need to learn the translation and mapping rules from.
        Then I will give you another Kusto query, you should translate it into Spark SQL based on what you have learned.

        Variable[number], String[number] and FunctionOrTable[number] are placeholders for variable, string, function or table in both languages.
        For example, Variable2 refers to second variable declared in the query. String4 refers to fourth string defined in the query. FunctionOrTable9 refers to nineth table or user-defined function being called in the query.
        
        Your output should follow the format below:
        <sparksql>spark sql query</sparksql>"""

    def _generate_llm_prompt(self, kusto_query, similar_queries):
        pairs_text = """"""
        for i, sq in enumerate(similar_queries):
            kql = sq['kql']
            sql = sq['sql']
            text = """Pair {}:

Kusto:

{}

Spark SQL:

{}

""".format(i + 1, kql, sql)
            pairs_text += text

        llm_prompt = """Given a (Kusto, Spark SQL) pair below:

{}

Translate below Kusto into Spark SQL:

{}""".format(pairs_text, kusto_query)

        return llm_prompt
