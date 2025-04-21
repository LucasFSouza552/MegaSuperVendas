# Aluno: Lucas Felipe de Souza

# Bibliotecas

from fuzzywuzzy import fuzz
import pandas as pd
import re
from datetime import datetime
import os

# Ler CSV

def readCsv(file):
	"""
	Lê um arquivo CSV e retorna um DataFrame do pandas.
	"""
	try:
		print(f"Lendo o arquivo: {file}")
		# Verifica se o arquivo existe e é um CSV
		if not file.endswith('.csv'):
			raise ValueError("O arquivo não é um CSV válido.")
		# Lê o arquivo CSV
		df = pd.read_csv(file)
		return df
	except Exception as e:
		print(f"Erro ao ler o arquivo: {e}")
		return None

# Remover espaços em brancos

def clean_whitespace(df):
	"""
	Remove espaços em branco no início e no final de todas as strings no DataFrame.
	"""
	try:
		# Aplica o strip em cada valor de string em cada coluna
		return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x) if col.dtype == 'object' else col)
	except Exception as e:
		print(f"Erro ao limpar espaços em branco: {e}")
		return df

# Normaliza a coluna status

def normalize_status(df):
	"""
	Normaliza a coluna 'status' para padronizar as expressões de status.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com a coluna 'status' normalizada.
	"""
	# Dicionário de mapeamento para normalizar os status
	status_map = {
		'Pagamento Confirmado': 'Pagamento Confirmado',
		'Pgto Confirmado': 'Pagamento Confirmado',
		'PC': 'Pagamento Confirmado',
		'Pago': 'Pagamento Confirmado',
		'Entregue': 'Entregue',
		'Entg': 'Entregue',
		'Entregue com Sucesso': 'Entregue',
		'Em Separação': 'Em Separação',
		'Sep': 'Em Separação',
		'Separando': 'Em Separação',
		'Aguardando Pagamento': 'Aguardando Pagamento',
		'Aguardando Pgto': 'Aguardando Pagamento',
		'aguardando pagamento': 'Aguardando Pagamento',
		'AP': 'Aguardando Pagamento',
		'Em Transporte': 'Em Transporte',
		'Transp': 'Em Transporte',
		'Transportando': 'Em Transporte',
	}

	# Aplicar o mapeamento para normalizar os valores da coluna 'status'
	df['status'] = df['status'].str.strip().map(status_map).fillna(df['status'])

	return df

# Remove caracteres especiais

def remove_special_characters(df, columns):
	"""
	Remove caracteres especiais de colunas específicas em um DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	columns (list): Lista de colunas onde os caracteres especiais serão removidos.
	
	Retorna:
	pd.DataFrame: O DataFrame com os caracteres especiais removidos.
	"""
	try:
		for column in columns:
			df[column] = df[column].apply(
				lambda x: re.sub(r'[^\w\s]', '', x) if isinstance(x, str) else x
			)
	except Exception as e:
		print(f"Erro ao remover caracteres especiais: {e}")
	return df

# Normaliza os nomes dos produtos

def compare_and_normalize_products(df, column='produto', threshold=60):
	"""
	Compara e normaliza os nomes dos produtos em um DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	column (str): O nome da coluna de produtos a ser normalizada.
	threshold (int): O limiar de similaridade para agrupar produtos.
	
	Retorna:
	pd.DataFrame: O DataFrame com os nomes dos produtos normalizados.
	"""
	unique_products = df[column].dropna().unique()
	product_mapping = {}

	for product in unique_products:
		if product in product_mapping:
			continue
		similar_products = [other for other in unique_products if fuzz.ratio(product, other) >= threshold]
		most_frequent_product = df[df[column].isin(similar_products)][column].mode()[0]

		for similar in similar_products:
			product_mapping[similar] = most_frequent_product

	df[column] = df[column].map(product_mapping)
	return df

# Normaliza os valores monetários

def normalize_monetary_values(df, column):
	"""
	Normaliza os valores monetários em uma coluna específica de um DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	column (str): O nome da coluna a ser normalizada.
	
	Retorna:
	pd.DataFrame: O DataFrame com a coluna normalizada.
	"""
	try:
		# Remove espaços em branco e outros caracteres não numéricos, exceto vírgula ou ponto
		df[column] = df[column].str.replace(r"[^\d,\.]", "", regex=True)

		# Substitui a vírgula por ponto para garantir que os valores possam ser convertidos corretamente
		df[column] = df[column].str.replace(",", ".", regex=False)

		# Converte os valores para numérico, substituindo erros por NaN
		df[column] = pd.to_numeric(df[column], errors="coerce")

		# Arredonda os valores para duas casas decimais
		df[column] = df[column].round(2)
	except Exception as e:
		print(f"Erro ao normalizar valores monetários na coluna '{column}': {e}")

	return df

# Corrige a capitalização

def correct_text_capitalization(df):
	"""
	Corrige a capitalização dos campos de texto em um DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com os campos de texto corrigidos.
	"""
	campos_texto = ['cliente', 'produto', 'status', 'cidade', 'pais', 'pagamento', 'vendedor', 'marca']
	for coluna in campos_texto:
		df[coluna] = df[coluna].str.title().str.strip()
	return df

# Preenche valores ausentes na coluna 'vendedor'

def fill_missing_vendedor(df):
	"""
	Preenche valores ausentes na coluna 'vendedor' com a moda por 'id_da_compra'.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com a coluna 'vendedor' preenchida.
	"""
	df['vendedor'] = df.groupby('id_da_compra')['vendedor'].transform(
		lambda x: x.mode().iloc[0] if not x.mode().empty else x
	)
	return df

# Limpa e padroniza a coluna de valor

def normalize_price_data(df):
	"""
	Limpa e padroniza a coluna de valor:
	- Converte para número.
	- Preenche valores nulos com a mediana por produto + marca.
	- Remove outliers por produto + marca.
	- Arredonda os valores para duas casas decimais.
	
	Parâmetros:
	df (pd.DataFrame): DataFrame com colunas 'produto', 'marca' e 'preco'.
	
	Retorna:
	pd.DataFrame: DataFrame com valores corrigidos.
	"""

	# 1. Converte a coluna de preço para float, forçando erros para NaN
	df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

	# 2. Preenche valores nulos com a mediana por produto + marca
	df['valor'] = df.groupby(['produto', 'marca'])['valor'].transform(
		lambda x: x.fillna(x.median())
	)

	# 3. Remove outliers dentro de cada grupo (produto + marca)
	def remove_outliers(group):
		Q1 = group['valor'].quantile(0.25)
		Q3 = group['valor'].quantile(0.75)
		IQR = Q3 - Q1
		lower = Q1 - 1.5 * IQR
		upper = Q3 + 1.5 * IQR
		return group[(group['valor'] >= lower) & (group['valor'] <= upper)]

	df = df.groupby(['produto', 'marca'], group_keys=False).apply(remove_outliers)

	# 4. Arredonda os preços
	df['valor'] = df['valor'].round(2)

	return df


# Preenche valores ausentes em colunas

def fill_missing_values(df, columns):
	"""
	Preenche valores ausentes em colunas específicas de um DataFrame usando a média e a mediana.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	columns (list): Lista de colunas para preencher valores ausentes.
	
	Retorna:
	pd.DataFrame: O DataFrame com os valores ausentes preenchidos.
	"""
	def calculate_mean_median(column):
		return df[column].agg(['mean', 'median']).mean()

	try:
		for column in columns:
			df[column] = df[column].fillna(calculate_mean_median(column))
	except Exception as e:
		print(f"Erro ao preencher valores ausentes: {e}")
	return df


# Convertendo para valores numéricos 

def normalize_numeric_columns(df, columns):
	"""
	Normaliza colunas numéricas, convertendo para valores numéricos.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	columns (list): Lista de colunas a serem normalizadas.
	
	Retorna:
	pd.DataFrame: O DataFrame com as colunas normalizadas.
	"""
	try:
		for column in columns:
			df[column] = pd.to_numeric(df[column], errors='coerce')
	except Exception as e:
		print(f"Erro ao normalizar colunas numéricas: {e}")
	return df

# Normaliza as colunas de data e hora

def normalize_datetime_columns(df):
	"""
	Normaliza as colunas de data e hora em um DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo as colunas 'data' e 'hora'.
	
	Retorna:
	pd.DataFrame: O DataFrame com as colunas 'data' e 'hora' normalizadas.
	"""
	try:
		df['data'] = pd.to_datetime(df['data'], errors='coerce') # erros viram NaT
		df['hora'] = pd.to_datetime(df['hora'], format='%H:%M:%S', errors='coerce').dt.time
	except Exception as e:
		print(f"Erro ao normalizar colunas de data/hora: {e}")
	return df

# Corrige os CEPs

def correct_cep_format(df):
	"""
	Corrige os CEPs para manter o padrão com hífen.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo a coluna 'cep'.
	
	Retorna:
	pd.DataFrame: O DataFrame com os CEPs corrigidos.
	"""
	try:
		df['cep'] = df['cep'].str.replace(r'\D', '', regex=True).str.zfill(8)
		df['cep'] = df['cep'].str[:5] + '-' + df['cep'].str[5:]
	except Exception as e:
		print(f"Erro ao corrigir o formato dos CEPs: {e}")
	return df

# Corrige tipo das colunas

def correct_column_formats(df):
	"""
	Corrige os formatos das colunas no DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com os formatos corrigidos.
	"""
	try:
		# Garantir que colunas numéricas estejam no formato correto
		numeric_columns = ['valor', 'quantidade', 'total', 'frete']
		for column in numeric_columns:
			df[column] = pd.to_numeric(df[column], errors='coerce')

		# Garantir que colunas de texto estejam no formato string e remover espaços
		text_columns = ['status', 'cep', 'pagamento']
		for column in text_columns:
			df[column] = df[column].astype(str).str.strip()

		# Garantir que colunas como 'produto', 'marca', 'vendedor', 'cliente', 'pais', 'cidade', 'estado' sejam categóricas
		categorical_columns = ['produto', 'marca', 'vendedor', 'cliente', 'pais', 'cidade', 'estado']
		for column in categorical_columns:
			df[column] = df[column].astype('category')

	except Exception as e:
		print(f"Erro ao corrigir os formatos das colunas: {e}")

	return df

#  Calcula o valor total do pedido

def calculate_total(df):
	"""
	Calcula o total como valor * quantidade + frete, 
	verificando se as colunas 'valor' e 'quantidade' estão definidas.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com a coluna 'total' atualizada.
	"""
	if 'valor' in df.columns and 'quantidade' in df.columns:
		df['total'] = df['valor'] * df['quantidade'] + df['frete']
		# Arredondar a coluna 'total' para 2 casas decimais
		df['total'] = df['total'].round(2)
	else:
		print("As colunas 'valor' e/ou 'quantidade' não estão definidas no DataFrame.")
	return df


# Preenche o valor do frete

def fill_frete_by_cep(df, moda_cep):
	"""
	Preenche os valores de frete ausentes com base no CEP.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	moda_cep (pd.Series): Série com os valores de CEP e frete mais comuns.
	
	Retorna:
	pd.DataFrame: O DataFrame com os valores de frete preenchidos.
	"""
	try:
		df['frete'] = df.apply(
			lambda row: moda_cep.get(row['cidade'], row['frete']) if pd.isnull(row['frete']) else row['frete'], axis=1
		)
	except Exception as e:
		print(f"Erro ao preencher o frete: {e}")
	return df


# Trata os dados indísponiveis

def handle_missing_values(df):
	"""
	Trata os dados faltantes no DataFrame, removendo ou preenchendo conforme necessário.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com dados faltantes tratados.
	"""
	# Remover linhas com dados faltantes em colunas críticas
	df = df.dropna(subset=['valor', 'quantidade', 'total'])

	# Preencher dados faltantes com valores padrão em outras colunas
	df['frete'] = df['frete'].fillna(0) # Exemplo: preencher com 0
	df['status'] = df['status'].fillna('Desconhecido') # Exemplo: preencher com valor padrão
	df['cep'] = df['cep'].fillna('00000-000') # Exemplo: preencher com um CEP padrão
	df['pagamento'] = df['pagamento'].fillna('Não Especificado') # Exemplo

	return df

def handle_inconsistent_values(df):
	"""
	Trata valores inconsistentes nas colunas do DataFrame.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame contendo os dados.
	
	Retorna:
	pd.DataFrame: O DataFrame com valores inconsistentes tratados.
	"""
	# Corrigir valores inconsistentes em colunas numéricas
	df['valor'] = df['valor'].apply(lambda x: max(x, 0)) 
	df['quantidade'] = df['quantidade'].apply(lambda x: max(x, 0)) 
	df['frete'] = df['frete'].apply(lambda x: max(x, 0))

	return df

# Corrige as marcas de acordo com o produto

def resolve_product_brand_discrepancies(df):
	"""
	Corrige inconsistências entre 'produto' e 'marca' com base na moda da marca para cada produto.
	
	Parâmetros:
	df (pd.DataFrame): DataFrame com as colunas 'produto' e 'marca'.
	
	Retorna:
	pd.DataFrame: DataFrame com inconsistências corrigidas.
	"""
	# Cópia para não modificar o original diretamente
	df_corrigido = df.copy()

	# Remove linhas com 'produto' ou 'marca' nulos para o agrupamento
	df_validos = df_corrigido.dropna(subset=['produto', 'marca'])

	# Calcula a moda (marca mais comum) para cada produto
	marca_mais_comum = df_validos.groupby('produto')['marca'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "Desconhecido")

	# Cria uma coluna com a marca esperada
	df_corrigido['marca_esperada'] = df_corrigido['produto'].map(marca_mais_comum)

	# Substitui a marca incorreta pela marca esperada (se for diferente e ambos não forem nulos)
	df_corrigido.loc[
		df_corrigido['marca'].notna() &
		df_corrigido['marca_esperada'].notna() &
		(df_corrigido['marca'] != df_corrigido['marca_esperada']),
		'marca'
	] = df_corrigido['marca_esperada']

	# Remove a coluna auxiliar
	df_corrigido.drop(columns=['marca_esperada'], inplace=True)

	return df_corrigido

# Salva o dataframe em CSV

def save_cleaned_dataframe(df, file_path):
	"""
	Salva o DataFrame limpo em um arquivo CSV.
	
	Parâmetros:
	df (pd.DataFrame): O DataFrame a ser salvo.
	file_path (str): O caminho do arquivo onde o DataFrame será salvo.
	"""
	try:
		df.to_csv(file_path, index=False)
		print(f"DataFrame salvo com sucesso em {file_path}")
	except Exception as e:
		print(f"Erro ao salvar o DataFrame: {e}")

# Gera um relatório de mudanças entre dois DataFrames
def generate_dataframe_change_report(df_before, df_after, primary_key='id_da_compra'):
	"""
	Gera um relatório completo das alterações entre dois DataFrames.
	
	Parâmetros:
	- df_before: DataFrame original (antes das alterações)
	- df_after: DataFrame modificado (após alterações)
	- primary_key: Nome da coluna chave para comparação (padrão: 'id_da_compra')
	
	Retorna:
	- String formatada em Markdown com o relatório completo
	"""

	def format_section(title, content):
		return f"### {title}\n{content}\n"

	def format_table(headers, rows):
		header_row = "| " + " | ".join(headers) + " |\n"
		separator_row = "| " + " | ".join(["-" * len(h) for h in headers]) + " |\n"
		data_rows = "\n".join("| " + " | ".join(map(str, row)) + " |" for row in rows)
		return header_row + separator_row + data_rows

	# Cabeçalho do relatório
	report = f"# Relatório de Alterações nos Dados\n**Data de geração:** {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"

	report += format_section("📊 Estatísticas Básicas", f"- Registros antes: {len(df_before)}\n- Registros depois: {len(df_after)}")

	# 2. Comparação de registros
	if primary_key in df_before.columns and primary_key in df_after.columns:
		added = set(df_after[primary_key]) - set(df_before[primary_key])
		removed = set(df_before[primary_key]) - set(df_after[primary_key])

		content = f"- Registros adicionados: {len(added)}\n- Registros removidos: {len(removed)}"
		if removed:
			content += "\n\n#### 🗑️ IDs Removidos\n```\n"
			content += "\n".join(map(str, list(removed)[:10])) # Mostra até 10 IDs
			if len(removed) > 10:
				content += f"\n... e mais {len(removed) - 10} registros\n"
			content += "\n```"
		report += format_section("📝 Mudanças nos Registros", content)


	report += format_section("✏️ Alterações nos Valores", "")

	nulls_before = df_before.isnull().sum()
	nulls_after = df_after.isnull().sum()
	null_changes = (nulls_before - nulls_after).sort_values(ascending=False)
	null_changes = null_changes[null_changes != 0]

	if not null_changes.empty:
		rows = [(col, count) for col, count in null_changes.items()]
		report += format_section("🔄 Mudanças em Valores Nulos", format_table(["Coluna", "Nulos removidos"], rows))
	else:
		report += "- Nenhuma mudança significativa em valores nulos\n"

	if 'status' in df_before.columns and 'status' in df_after.columns and primary_key in df_before.columns and primary_key in df_after.columns:
		status_comparison = pd.merge(
			df_before[[primary_key, 'status']].rename(columns={'status': 'status_antes'}),
			df_after[[primary_key, 'status']].rename(columns={'status': 'status_depois'}),
			on=primary_key
		)
		changed_status = status_comparison[status_comparison['status_antes'] != status_comparison['status_depois']]

		if not changed_status.empty:
			change_summary = changed_status.groupby(['status_antes', 'status_depois']).size().reset_index(name='quantidade')
			change_summary = change_summary.sort_values('quantidade', ascending=False)
			rows = change_summary.values.tolist()
			report += format_section("🔄 Transformações na Coluna 'status'", format_table(["Status Anterior", "Status Atual", "Registros"], rows))
		else:
			report += "- ✅ Nenhuma alteração nos valores da coluna 'status' encontrada\n"

	report += format_section("⚠️ Possíveis Inconsistências", "")

	if 'produto' in df_after.columns and 'marca' in df_after.columns:
		marca_por_produto = df_after.groupby('produto')['marca'].agg(pd.Series.mode)
		inconsistentes = df_after[~df_after.apply(
			lambda x: x['marca'] in marca_por_produto[x['produto']], axis=1)]

		if not inconsistentes.empty:
			rows = [
				(row.get(primary_key, ''), row['produto'], row['marca'], marca_por_produto[row['produto']])
				for _, row in inconsistentes.head(5).iterrows()
			]
			report += format_section("🏷️ Inconsistências produto-marca", format_table(["ID", "Produto", "Marca", "Marca esperada"], rows))
			if len(inconsistentes) > 5:
				report += f"\n*Mostrando 5 de {len(inconsistentes)} inconsistências...*\n"

	report += "\n---\nRelatório gerado automaticamente\n"

	return report

# Lê o arquivo CSV e cria um DataFrame
file_path = os.path.join(os.getcwd(), 'dataframe', 'vendas_modificado.csv')
df = readCsv(file_path)
df_mod = None
# Verifica se o DataFrame foi criado
if df is not None:
	print("DataFrame criado com sucesso.")
	df_mod = df.copy()
else:
	print("Erro ao criar o DataFrame. Verifique o arquivo CSV.")

# Aplicando a função ao DataFrame
df_mod = clean_whitespace(df_mod)

# Normaliza a coluna 'status' do DataFrame
if df_mod is not None and 'status' in df_mod.columns:
	print("Normalizando a coluna 'status'...")
	df_mod = normalize_status(df_mod)

# Chamando a função para remover caracteres especiais das colunas desejadas
if df_mod is not None and 'produto' in df_mod.columns:
	print("Removendo caracteres especiais da coluna 'produto'...")
	df_mod = remove_special_characters(df_mod, ['produto'])

# Aplicando a função para normalizar os nomes dos produtos
if df_mod is not None and 'produto' in df_mod.columns:
	print("Normalizando os nomes dos produtos...")
	df_mod = compare_and_normalize_products(df_mod, column='produto')

# Normaliza os valores monetários
if df_mod is not None and 'valor' in df_mod.columns:
	print("Normalizando os valores monetários...")
	df_mod = normalize_monetary_values(df_mod.copy(), "valor")

# Chamando a função para corrigir a capitalização
if df_mod is not None:
	print("Corrigindo a capitalização dos campos de texto...")
	df_mod = correct_text_capitalization(df_mod)

# Preenche valores ausentes na coluna 'vendedor'
if df_mod is not None and 'vendedor' in df_mod.columns:
	print("Preenchendo valores ausentes na coluna 'vendedor'...")
	df_mod = fill_missing_vendedor(df_mod)

# Limpa e padroniza a coluna de valor
if df_mod is not None and 'valor' in df_mod.columns:
	print("Normalizando os valores monetários na coluna 'valor'...")
	df_mod = normalize_price_data(df_mod)

# Chamando a função para preencher valores ausentes
if df_mod is not None and 'valor' in df_mod.columns and 'frete' in df_mod.columns:
	print("Preenchendo valores ausentes nas colunas 'valor' e 'frete'...")
	df_mod = fill_missing_values(df_mod, ['valor', 'frete'])

# Chamando a função para normalizar as colunas 'frete' e 'total'
if df_mod is not None and 'frete' in df_mod.columns and 'total' in df_mod.columns:
	print("Normalizando as colunas 'frete' e 'total'...")
	df_mod = normalize_numeric_columns(df_mod, ['valor', 'quantidade', 'frete', 'total'])


# Chamando a função para normalizar as colunas de data e hora
if df_mod is not None and 'data' in df_mod.columns and 'hora' in df_mod.columns:
	print("Normalizando as colunas de data e hora...")
	df_mod = normalize_datetime_columns(df_mod)

# Chamando a função para corrigir os CEPs
if df_mod is not None and 'cep' in df_mod.columns:
	print("Corrigindo o formato dos CEPs...")
	df_mod = correct_cep_format(df_mod)

# Chamando para calcular o total
if df_mod is not None and 'valor' in df_mod.columns and 'quantidade' in df_mod.columns and 'frete' in df_mod.columns:
	print("Calculando o total...")
	df_mod = calculate_total(df_mod)

# Removendo linhas onde 'vendedor' é NaN
if df_mod is not None and 'vendedor' in df_mod.columns:
	print("Removendo linhas onde 'vendedor' é NaN...")
	df_mod = df_mod.dropna(subset=['vendedor'])

# Calculando a moda do frete por cidade
if df_mod is not None and 'cidade' in df_mod.columns and 'frete' in df_mod.columns:
	print("Calculando a moda do frete por cidade...")
	moda_cep = df_mod.groupby('cidade')['frete'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)

# Aplicando a função ao DataFrame
if df_mod is not None and 'frete' in df_mod.columns:
	print("Preenchendo os valores de frete ausentes com base no CEP...")
	df_mod = fill_frete_by_cep(df_mod, moda_cep)
	df_mod['frete'] = df_mod['frete'].round(2)

# Corrigindo os formatos das colunas
if df_mod is not None:
	print("Corrigindo os formatos das colunas...")
	df_mod = correct_column_formats(df_mod)
	print("Removendo dados faltantes...")
	df_mod = handle_missing_values(df_mod)
	print("Corrigindo valores inconsistentes...")
	df_mod = handle_inconsistent_values(df_mod)
	print("Corrigindo inconsistências entre produto e marca...")
	df_mod = resolve_product_brand_discrepancies(df_mod)

# Remover dados duplicados
if df_mod is not None:
	print("Removendo dados duplicados...")
	df_mod = df_mod.drop_duplicates()
 
 # Transforma o header (nomes das colunas) em maiúsculas
df.columns = df.columns.str.upper()
df_mod.columns = df_mod.columns.str.upper()

# Chamando a função para salvar o DataFrame limpo
if df_mod is not None:
	print("Salvando o DataFrame limpo...")
	file_csv = os.path.join(os.getcwd(), 'result', 'compras_normalizadas.csv')
	save_cleaned_dataframe(df_mod, file_csv)

# Gerando o relatório de alterações
if df is not None and df_mod is not None:
	print("Gerando o relatório de alterações...")
	relatorio = generate_dataframe_change_report(df, df_mod)
	print(relatorio)
	# Salvando o relatório em um arquivo Markdown
	file_relatorio = os.path.join(os.getcwd(), 'result', 'relatorio_alteracoes.md')
	with open(file_relatorio, 'w', encoding='utf-8') as f:
		f.write(relatorio)
		print("Relatório salvo em 'relatorio_alteracoes.md'")