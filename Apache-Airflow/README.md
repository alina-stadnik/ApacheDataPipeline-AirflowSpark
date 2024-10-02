### **Apache Airflow**

O [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/2.3.2/index.html) (ou simplesmente Airflow) é uma plataforma para criar, agendar e monitorar fluxos de trabalho de forma programática. Quando os fluxos de trabalho são definidos como código, eles se tornam mais fáceis de manter, versionáveis, testáveis e colaborativos.

**Princípios do Airflow:**

- **Dinâmico**: Os pipelines do Airflow são configurados como código (Python), permitindo a geração dinâmica de pipelines. Isso permite escrever códigos que instanciam pipelines dinamicamente.
- **Extensível**: Permite definir facilmente operadores próprios e executores e estende a biblioteca para que ela se ajuste ao nível de abstração adequado ao ambiente em questão.
- **Elegante**: Os pipelines do Airflow são enxutos e explícitos. A parametrização dos scripts é incorporada ao núcleo do Airflow usando o mecanismo de modelagem Jinja.
- **Escalável**: O Airflow tem uma arquitetura modular e usa uma fila de mensagens para orquestrar um número arbitrário de trabalhadores.

O Airflow é comumente usado para processar dados e não devem passar grandes quantidades de dados de uma tarefa para a outra (embora as tarefas possam passar metadados usando o recurso [XCom](https://airflow.apache.org/docs/apache-airflow/2.3.2/concepts/xcoms.html#xcoms) (formato nativo definido por chave, valor e data) do Airflow). Para tarefas de grande volume e com uso intensivo de dados, uma prática recomendada é delegar a serviços externos especializados nesse tipo de trabalho. O Airflow não é uma solução de fluxo contínuo, mas é frequentemente usado para processar dados em tempo real, extraindo dados de fluxos em lotes.

1. Ativação do venv:

Ativação direta do venv utilizando o comando `source venv/bin/activate`.

2. Instalação do Airflow:

A instalação pode ser um pouco complicada, pois o Airflow é uma **biblioteca** e um **aplicativo**. As bibliotecas geralmente mantêm suas dependências abertas, e os aplicativos geralmente as fixam. Isso significa que o pip install apache-airflow não funcionará de vez em quando ou produzirá uma instalação inutilizável do Airflow. Dessa forma, para a instalação alguns conjuntos de restrições serão mantidas e especificadas na URL.

`AIRFLOW_VERSION=2.3.2`\
`PYTHON_VERSION=3.9`\
`pip3 install "apache-airflow[postgres,celery,redis]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`

3. Exportação da variável antes de inicializar o Airflow:

`export AIRFLOW_HOME=$(pwd)/airflow_pipeline`

A **variável é exportada na pasta principal do projeto e deixando o diretório airflow_pipeline referenciado**, para que os arquivos do Airflow não se misturem com o restante do projeto.

> Essa exportação é necessária todas as vezes para que o Airflow posssa ser iniciado.

4. Inicialização do Airflow:

`airflow standalone`

A hospedagem da página por padrão fica no **localhost:8080**. O usuário e a senha para acessar o web server aparecem no terminal após esse comando. Essa senha também é gerada no diretório 'airflow_pipeline' no arquivo password.txt. Todas as definições serão configuradas nesse web server. 

5. Criação da Conexão.

Admin ➞ Connections ➞ + ➞ add a new record ➞ implementação no código

New record:
- **Connection Id:** Nome para a conexão (toda vez que referencia a conexão no código é por esse id);
- **Connection Type:** por padrão é e-mail, mas é em relação a conexão da API utilizada (no caso do projeto HTTP);
- **Host:** endpoint da API utilizada (API do twitter/labdados.com);
- **Extra:** em caso de token é aqui que será adicionado (formato de json).