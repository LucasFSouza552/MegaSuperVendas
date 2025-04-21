Projeto de Limpeza de Dados - MegaSuper Vendas

Projeto de limpeza de dados da **MegaSuper Vendas**, uma startup de e-commerce com um banco de dados desorganizado que precisa ser transformado em algo confiável e pronto para análises.

---

## 🗂 Sobre o Projeto

Você encontrará no repositório um script Python que realiza as seguintes tarefas:

- Conversão e padronização de **datas e horários**;
- Tratamento e conversão de **valores numéricos**;
- **Remoção de duplicatas**;
- **Imputação ou remoção** de valores ausentes;
- **Validação e correção** dos cálculos na coluna `total`.

---
## Pré-requisitos

Antes de começar, você precisará ter instalado:

- [Python 3.11+](https://www.python.org/downloads/)
- [pip](https://pip.pypa.io/en/stable/)

---

## ⚙️ Como Iniciar

### 1. Clone o repositório

```bash
git clone https://github.com/LucasFSouza552/MegaSuperVendas.git
cd MegaSuperVendas
```

### Crie um ambiente virtual
```bash
python -m venv venv
```

### Instale as dependências
```bash
pip install -r requirements.txt
```

### Execute o script principal
```bash
python src/main.py
```

## Resultado Esperado
Após a execução do script, você terá um novo DataFrame limpo e pronto para análises, com:

Tipos de dados padronizados;

Colunas numéricas corrigidas;

Valores ausentes tratados;

Total de vendas recalculado corretamente.

## ✍️ Autor
Lucas Felipe de Souza  
Aluno de Análise e Desenvolvimento de Sistemas