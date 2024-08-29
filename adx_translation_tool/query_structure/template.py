import re
import sqlparse.keywords


class KustoTemplate():
    def __init__(self, function_list, table_list, kusto_keywords_path):
        self._function_list = function_list
        self._table_list = table_list
        self._kusto_keywords_path = kusto_keywords_path
        self._kusto_keywords = self._get_kusto_keywords()
        self._function_and_table_pattern = r'(?<!-)\b(\w+)\s*(?=\(|\s)'
        self._variable_pattern = r'(?<!-)\b[_a-zA-Z][_a-zA-Z0-9]*\b'
        self._string_pattern = r'([\"\'])(?:\\.|(?!\1).)*\1'
        self._function_and_table_replaced_by = 'FunctionOrTable'
        self.variable_replaced_by = 'Variable'
        self.string_replaced_by = 'String'
        self._replaced_functions_and_tables = []
        self._replaced_variables = []
        self._replaced_strings = []

    def to_template(self, query):
        query = KustoTemplate.clean(query)
        query = self._replace(query)
        return query

    def _replace(self, query):
        self._replaced_functions_and_tables = []
        self._replaced_variables = []
        self._replaced_strings = []
        query = re.sub(self._function_and_table_pattern,
                       self._process_function_and_table, query)
        query = re.sub(self._variable_pattern, self._process_variable, query)
        query = re.sub(self._string_pattern, self._process_string, query)
        return query

    @staticmethod
    def clean(query):
        # Remove new lines in front and end.
        query = query.strip('\n')
        # Remove spaces in front and end.
        query = query.strip()
        # Remove unnecessary indents.
        query = re.sub(r'\n\s+', '\n', query)
        # Remove comments.
        query = KustoTemplate.remove_comments(query)
        # Remove multiple contiguous new line symbol.
        query = re.sub(r'\n+', '\n', query)

        return query

    @staticmethod
    def remove_comments(query):
        url_pattern = r'(http:\/\/|https:\/\/)[\w\-]+(\.[\w\-]+)+([\/\w\-.]*)*'
        comment_pattern = r'\/\/.*'
        lines = query.splitlines()
        cleaned_lines = []

        for line in lines:
            if re.search(url_pattern, line):
                cleaned_lines.append(line)
            else:
                cleaned_line = re.sub(comment_pattern, '', line).rstrip()
                cleaned_lines.append(cleaned_line)

        return '\n'.join(cleaned_lines)

    def _process_function_and_table(self, match):
        if match.group() in self._function_list or match.group() in self._table_list:
            if match.group() in self._replaced_functions_and_tables:
                return self._function_and_table_replaced_by + str(self._replaced_functions_and_tables.index(match.group()) + 1)
            else:
                self._replaced_functions_and_tables.append(match.group())
                return self._function_and_table_replaced_by + str(len(self._replaced_functions_and_tables))
        else:
            return match.group()

    def _process_variable(self, match):
        if match.group().upper() in self._kusto_keywords or self._function_and_table_replaced_by in match.group():
            return match.group()
        else:
            if match.group() in self._replaced_variables:
                return self.variable_replaced_by + str(self._replaced_variables.index(match.group()) + 1)
            else:
                self._replaced_variables.append(match.group())
                return self.variable_replaced_by + str(len(self._replaced_variables))

    def _process_string(self, match):
        if match.group() in self._replaced_strings:
            return self.string_replaced_by + str(self._replaced_strings.index(match.group()) + 1)
        else:
            self._replaced_strings.append(match.group())
            return self.string_replaced_by + str(len(self._replaced_strings))

    def _get_kusto_keywords(self):
        with open(self._kusto_keywords_path) as f:
            kql_keywords = []
            for line in f.readlines():
                line = line.replace('\n', '')
                kql_keywords.append(line.upper())
            kql_keywords = list(set(kql_keywords))
            return kql_keywords


class SparkSQLTemplate():
    @staticmethod
    def clean(query):
        # Remove new lines in front and end.
        query = query.strip('\n')
        # Remove spaces in front and end.
        query = query.strip()
        # Remove unnecessary indents.
        query = re.sub(r'\n\s+', '\n', query)
        # Remove comments.
        query = re.sub(r'--.*', '', query)
        # Remove multiple contiguous new line symbol.
        query = re.sub(r'\n+', '\n', query)

        return query

    @staticmethod
    def format(query):
        return sqlparse.format(query, keyword_case='upper', reindent=True, use_space_around_operators=True)
