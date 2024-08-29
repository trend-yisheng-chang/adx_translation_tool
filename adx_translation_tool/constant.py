import os

OPEN_AI_BASE_URL = 'https://api.rdsec.trendmicro.com/prod/aiendpoint/v1/'
GROUND_TRUTH_JSON_PATH = os.path.join(
    os.path.dirname(__file__), 'ground_truths.json')
KUSTO_KEYWORD_TXT_PATH = os.path.join(
    os.path.dirname(__file__), 'kusto_keywords.txt')
