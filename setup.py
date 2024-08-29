from setuptools import setup, find_packages

setup(
    name='adx_translation_tool',
    version='0.1.0',
    author='Eason YS Chang',
    author_email='eason_ys_chang@trendmicro.com',
    description='A tool for translate KQL to SQL for dashboards migration from ADX to Databricks.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/trend-yisheng-chang/adx_translation_tool.git',
    packages=find_packages(),
    install_requires=['openai', 'sqlparse', 'pyspark', 'scikit-learn'],
    include_package_data=True,
    package_data={'adx_translation_tool': [
        'ground_truths.json', 'kusto_keywords.txt']},
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
