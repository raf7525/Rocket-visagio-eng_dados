# CineData Analytics

Pipeline de dados end-to-end construído no Databricks para estruturar um catálogo de filmes (base combinada TMDB/IMDb) seguindo a Arquitetura Medalhão.

**Stack:** Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog.

---

## Sumário

1. [Arquitetura](#arquitetura)
2. [O contexto: uma origem deliberadamente degradada](#o-contexto-uma-origem-deliberadamente-degradada)
3. [Princípio que orientou as decisões](#princípio-que-orientou-as-decisões)
4. [Problemas enfrentados e soluções](#problemas-enfrentados-e-soluções)
5. [Tratamento por tabela](#tratamento-por-tabela)
6. [Camada Gold: modelo dimensional](#camada-gold-modelo-dimensional)
7. [Avisos do otimizador do Databricks](#avisos-do-otimizador-do-databricks)
8. [Melhorias futuras consolidadas](#melhorias-futuras-consolidadas)
9. [Orquestração e execução](#orquestração-e-execução)

---

## Arquitetura

```
CSVs (Volume) + API PTAX/BCB
        ↓
   BRONZE  dados brutos, sem alteração, em Delta com append e ingestion_datetime
        ↓
   SILVER  colunas em português, tipagem correta, limpeza e regras de negócio
        ↓
   GOLD    Star Schema, tabela de contexto para RAG e consultas analíticas
```

**Notebooks**

| Notebook | Responsabilidade |
|---|---|
| `Landing_to_Bronze` | Ingestão dos 5 CSVs e da cotação do dólar via API do Banco Central |
| `Bronze_to_Silver` | Limpeza, tipagem, tradução de colunas e aplicação das regras de negócio |
| `Silver_to_Gold` | Modelagem dimensional, tabela de contexto para IA e consultas analíticas |

**Tabelas produzidas**

| Camada | Tabelas |
|---|---|
| Bronze | `tb_movies_info`, `tb_movies_financials`, `tb_movies_metrics`, `tb_credits_and_tags`, `tb_movies_reviews`, `tb_cotacao_dolar` |
| Silver | `tb_mapa_id_filme`, `tb_cotacao_dolar`, `tb_info_filmes`, `tb_financeiro_filmes`, `tb_metricas_engajamento`, `tb_avaliacoes_usuarios`, `tb_generos`, `tb_pessoas_empresas` |
| Gold | `fact_movies_performance`, `dim_movies`, `dim_genres`, `dim_people`, `dim_companies`, `dim_reviews`, `bridge_movie_genre`, `bridge_movie_person`, `bridge_movie_company`, `gold_genai_movies_context` |

---

## O contexto: uma origem deliberadamente degradada

Os arquivos de entrada foram fornecidos intencionalmente sujos e fragmentados. Além dos problemas esperados (valores textuais representando ausência, datas em formatos variados, separadores inconsistentes), a base apresenta duas classes de corrupção mais graves, que condicionaram boa parte das decisões deste projeto:

**Deslocamento de colunas (Column Shift).** Aspas mal escapadas no CSV original fizeram campos de linhas inteiras se fundirem. Sinopses aparecem dentro da coluna de popularidade, idiomas dentro da coluna de gêneros, e em alguns casos o próprio campo de identificação do filme contém texto livre.

**Identidade de filmes fragmentada.** O mesmo filme está cadastrado sob múltiplos identificadores naturais distintos, com variações de título, data e sinopse. O caso extremo encontrado soma cerca de 60 registros para uma única obra.

A segunda classe é a mais problemática, porque não é um defeito de formatação: é um defeito de **granularidade**. Nenhuma limpeza de campo resolve, e ela distorce silenciosamente todas as métricas agregadas.

---

## Princípio que orientou as decisões

Diante de um valor suspeito, há duas condutas possíveis: descartar o registro inteiro, ou invalidar apenas o campo problemático e preservar o restante.

Este projeto adotou a segunda de forma consistente. Um filme com orçamento ilegível permanece no catálogo com orçamento nulo; uma avaliação com nota fora da escala mantém seu comentário; uma linha com sinopse destruída continua identificável pelo título.

A conduta oposta produziria uma base menor e aparentemente mais limpa, ao custo de perder silenciosamente informação recuperável. Numa origem tão degradada quanto esta, seria a diferença entre um catálogo de 97 mil filmes e um de algumas dezenas de milhares.

A exceção deliberada são os registros cuja **identidade** é irrecuperável: identificador não numérico na tabela financeira e valores fora do catálogo de gêneros. Nesses casos não há o que preservar, porque um registro que não pode ser associado a um filme não tem utilidade analítica.

---

## Problemas enfrentados e soluções

### 1. Identidade de filmes: o mesmo filme sob identificadores distintos

#### O problema

A consulta "qual ator teve mais participações nos últimos dois anos" depende de uma premissa que a base não sustenta: que cada filme tenha identidade única.

Na prática, o filme `Die Hart 2` aparece em cerca de 60 registros, com 9 variações de título, aproximadamente 10 datas de lançamento distintas, e metade deles sem sinopse. Como cada identificador é tratado como um filme diferente, um ator com uma única participação era contado uma vez por registro: Kevin Hart aparecia com 12 participações quando o número real é 5.

Não se trata de erro na consulta. A agregação está correta; o que está errado é a granularidade da dimensão que ela agrega. O mesmo defeito inflava a receita total, o volume por gênero e a contagem de linhas da `dim_movies`.

#### Por que a regra do enunciado não cobre

O enunciado pede unicidade por filme mantendo "a versão mais recente com base na data de ingestão". Essa regra resolve o caso do mesmo identificador reingerido várias vezes pelo modo `append`, e foi implementada. Ela não alcança registros com identificadores **diferentes** que representam a mesma obra, porque para o pipeline são filmes distintos.

#### As tentativas e o que cada uma quebrou

As variações não seguem um padrão único. Cada tentativa resolveu um caso e revelou outro:

1. **Agrupar por título normalizado** (minúsculas, sem pontuação). Resolveu caixa e separadores (`DIE HART 2: DIE HARTER`, `Die Hart 2 - Die Harter`, `Die Hart 2 : Die Harter`), mas fundiu filmes homônimos: 8 obras diferentes chamadas `Silence` lançadas entre 2016 e 2021, 7 chamadas `The Outsider`, além de `Inferno`, `Sundown`, `Collide` e `Genius`. Títulos curtos são reutilizados com frequência no cinema.

2. **Acrescentar a data de lançamento à chave.** Separou os homônimos corretamente. Exigiu parsear a data antes da comparação, porque a origem mistura formatos e `09-10-2016` é o mesmo dia que `2016-09-10`. Ainda assim, o título truncado (`Die Hart 2`, sem subtítulo) e o título localizado (`Duro de Atuar 2`, versão em português) permaneceram fora do grupo.

3. **Usar a sinopse como segunda chave.** Sendo texto longo e distintivo, a sinopse liga registros cujos títulos não têm nada em comum. Porém fundiu obras seriadas que compartilham o mesmo texto: `Ah, Wilderness: Part 1` com `Part 2`, e `Yuki Yuna Is a Hero: Washio Sumi Chapter 1/2/3`.

4. **Guarda por assinatura numérica do título.** Bloqueou a fusão das partes (`1` diferente de `2`), mas, sendo comparação de igualdade estrita, passou a separar variações legítimas: `Die Hart: Die Harter` (sem dígito) ficava fora do grupo de `Die Hart 2: Die Harter` (dígito `2`), mesmo com sinopse idêntica.

5. **Metade dos registros não tem sinopse.** Dos 60 registros do `Die Hart 2`, cerca de 30 têm o campo nulo, o que torna qualquer regra baseada em sinopse insuficiente isoladamente.

#### A solução

Uma tabela de mapeamento `silver.tb_mapa_id_filme` (`id_filme_original` para `id_filme_canonico`), construída em quatro passadas de evidência crescente. Todas as tabelas Silver traduzem seu identificador por esse mapa logo após a leitura da Bronze, antes de qualquer agregação. Assim os dados dos identificadores duplicados se consolidam sob o mesmo filme e a camada Gold funciona sem alteração.

| Passada | Chave de agrupamento | Objetivo |
|---|---|---|
| 1 | Título normalizado + data parseada | Une variações de caixa e pontuação, sem fundir homônimos |
| 2a | Sinopse com 80 caracteres ou mais + assinatura numérica | Une títulos diferentes com mesma sinopse, preservando obras seriadas |
| 2b | Sinopse, apenas para títulos sem dígito | Permite que variações sem numeral entrem no grupo, sem afetar `Part 1` contra `Part 2`, que têm dígito nos dois lados |
| 3 | Prefixo de 2 palavras + ano + assinatura numérica, apenas para registros sem sinopse utilizável | Absorve registros sem evidência textual própria; quem tem sinopse distinta não é tocado |
| 4 | Propagação por `canon_1` | Transitividade: se a passada 1 considerou dois registros o mesmo filme, ambos terminam no mesmo grupo mesmo que só um tenha migrado nas passadas seguintes |

**Critérios de segurança.** Título ou data ausentes não participam do agrupamento, o que evita fundir todos os registros incompletos entre si. A sinopse só é usada acima de 80 caracteres, porque texto curto não identifica uma obra. O identificador canônico é o menor do grupo, o que garante resultado determinístico entre execuções.

**Resultado.** Os 60 registros do `Die Hart 2` passaram a compor um único filme. `Die Hart` (2023) permaneceu separado pelo ano, e os filmes homônimos `Silence` continuaram distintos por terem sinopses próprias.

#### Por que a solução não é a ideal

Quatro fragilidades merecem registro explícito:

**É inferência, não resolução.** Um sistema correto identificaria filmes por uma chave estável e externa, como o identificador oficial do IMDb ou do TMDB. O que este pipeline faz é deduzir, a partir de semelhança de título, data e sinopse, quais registros descrevem a mesma obra. Toda inferência tem taxa de erro: dois registros de `Silence` de 2020, ambos sem sinopse, foram unificados sem que se possa afirmar que são o mesmo filme.

**Os limiares são empíricos.** O corte de 80 caracteres, o prefixo de duas palavras e a faixa de 1870 a 2030 foram calibrados observando os casos concretos desta base. Não há fundamento teórico por trás deles, em outro conjunto de dados precisariam ser recalibrados do zero, e não existe teste automatizado que detecte quando deixaram de ser adequados.

**A chave canônica não é estável ao longo do tempo.** O identificador canônico é o menor do grupo. Se uma carga futura trouxer um registro do mesmo filme com identificador inferior, o canônico muda, e com ele todas as surrogate keys derivadas na Gold. Em produção isso quebraria referências históricas em relatórios e dashboards.

**A propagação é aproximada.** As quatro passadas aplicam `min()` sucessivos, o que emula uma busca de componentes conexos sem de fato executá-la. Cadeias longas (A liga com B por título, B liga com C por sinopse, C liga com D por prefixo) podem não se fechar completamente. Foi exatamente esse tipo de lacuna que exigiu acrescentar a quarta passada, depois que um registro ficou órfão do próprio grupo.

#### Limitações conhecidas

- `Duro de Atuar 2` (título localizado) permanece como filme separado. Não compartilha título, prefixo nem sinopse com as demais variações, portanto não há evidência que o vincule.
- Dois registros `Silence` de 2020, ambos sem sinopse, foram unificados. Compartilham título e ano e não possuem nenhum atributo que os distinga. A ambiguidade é irredutível com os dados disponíveis.
- `Alien: Covenant - Prologue: Advent` e `Epilogue: Advent` são unificados quando compartilham data e sinopse, embora sejam peças distintas.

Em todos esses casos optou-se por não adicionar heurísticas mais agressivas, que resolveriam o caso isolado ao custo de reintroduzir fusões incorretas em outros pontos da base.

#### Melhorias

- **Usar `titulo_original` como sinal adicional no mapa.** O agrupamento olha apenas `title`. O registro `Duro de Atuar 2` ficou isolado por ser um título localizado, mas o campo `original_title` dele provavelmente contém `Die Hart 2`. Cruzar os dois campos resolveria essa classe inteira de casos sem nenhuma heurística nova. É a melhoria de maior retorno pelo menor esforço.
- **Componentes conexos de verdade**, com GraphFrames, tratando cada evidência como aresta de um grafo. Garante fechamento transitivo real em vez de aproximá-lo por passadas sucessivas.
- **Similaridade textual** (Levenshtein ou Jaccard sobre tokens) em vez de comparação exata de prefixo, com limiar calibrado. Capturaria erros de digitação que o prefixo fixo não alcança.
- **Chave canônica derivada de hash do conteúdo identificador**, ou tabela de identidades persistida que só recebe novos membros e nunca reatribui os existentes, resolvendo a instabilidade entre cargas.
- **Tabela de quarentena** para registros que nenhuma regra conseguiu resolver, permitindo revisão humana em vez de descarte ou fusão silenciosa.
- **Tabela de exceções manuais** mapeando casos conhecidos, como títulos localizados. Não é elegante, mas em integração de dados costuma ser mais honesto do que forçar uma heurística que quebra outros casos.

---

### 2. Sinopses destruídas pelo parsing: contenção de danos

#### O problema

A sinopse não sofre "sujeira" no sentido usual, de valores mal formatados. Ela foi **estruturalmente destruída**: aspas mal escapadas fizeram campos de linhas inteiras se fundirem.

```
...Bozkır: Kuşlara Bak Kuşlaratr2019-09-06,115,released,"Weapons dealers Abdullah..."
                                 ↑     ↑         ↑      ↑
                             idioma   data   duração  status   ← colunas grudadas
```

#### Por que não era possível limpar mais

Sinopse é texto livre: não há formato para validar, não há domínio fechado para comparar, não há tamanho esperado. A abordagem que funcionou nos gêneros, manter apenas o que consta de um catálogo, é inaplicável aqui.

Isso limita a detecção a **assinaturas estruturais** de corrupção, não a avaliação de conteúdo. Qualquer regra mais ampla seria destrutiva por natureza:

- Rejeitar sinopses com caracteres incomuns eliminaria filmes internacionais legítimos, cheios de acentuação e alfabetos não latinos.
- Rejeitar por tamanho descartaria sinopses curtas legítimas e ainda assim manteria as longas corrompidas.
- Rejeitar por presença de números eliminaria toda sinopse que cite um ano, o que é comum.

Com 14% dos registros já nulificados pelas duas regras conservadoras adotadas, endurecer o critério significaria perder sinopses reais em volume, justamente o conteúdo mais valioso para o time de IA, que é o consumidor final dessa coluna.

#### A solução: conter o dano

A detecção usa dois sinais independentes, avaliados sobre os valores **brutos**, antes de qualquer limpeza (caso contrário a assinatura da corrupção desapareceria):

- `idioma_original` fora do padrão de duas letras, indicando desalinhamento da linha inteira;
- presença, dentro da sinopse, da assinatura de uma linha de CSV grudada: data ISO seguida de duração e status separados por vírgula.

E a contenção seguiu três regras:

- Anular **apenas o campo comprovadamente contaminado**, preservando a linha e os demais atributos.
- Preservar o título mesmo nas linhas marcadas como corrompidas, por ser a única identificação legível do registro.
- Garantir, na tabela de contexto do RAG, um texto de substituição para sinopse ausente, de modo que nenhum filme desapareça da base vetorial por falta desse campo.

#### Limitações conhecidas

- Cerca de 14% dos registros ficam com sinopse nula. Parte já vinha vazia da origem; o restante são linhas cuja estrutura foi destruída no parsing.
- A detecção é heurística: linhas com desalinhamento sutil, que preservem um idioma válido e não apresentem a assinatura de CSV grudado, passam sem ser sinalizadas.

#### Melhoria principal: atacar a causa, não o efeito

Todo esse tratamento existe porque a leitura na camada Bronze usou as opções padrão do `spark.read.csv`. Boa parte da corrupção provavelmente não teria ocorrido com uma configuração de parsing adequada:

```python
spark.read \
  .option("header", "true") \
  .option("multiLine", "true") \
  .option("escape", '"') \
  .option("mode", "PERMISSIVE") \
  .option("columnNameOfCorruptRecord", "_registro_corrompido")
```

Com `multiLine` e `escape` configurados, sinopses que contêm aspas e quebras de linha seriam lidas corretamente, em vez de invadir as colunas seguintes. A coluna de registro corrompido permitiria quarentenar as linhas problemáticas já na ingestão, em vez de deixá-las contaminar toda a camada Silver.

Esta é, de longe, a melhoria mais impactante de todo o projeto: eliminaria a causa raiz de problemas que hoje são tratados individualmente em cinco tabelas diferentes.

---

### 3. Notação monetária abreviada: um erro de fator um milhão

#### O problema

A coluna de orçamento mistura notações na mesma base: `150000000`, `USD 150000000` e `34.0M` convivem lado a lado.

Uma versão inicial da limpeza removia todos os caracteres não numéricos, o que transformava `34.0M` em `34,00`. Um filme com orçamento real de 34 milhões de dólares era gravado com orçamento de 34 dólares.

O caso foi descoberto ao investigar um registro com orçamento de US$ 90,00 e margem de lucro de 473 milhões por cento. O valor original era `90.0M`.

#### A solução

A função de higienização monetária passou a validar o valor contra o formato esperado antes de convertê-lo, e a interpretar o sufixo de escala:

- textos conhecidos de ausência (`Unknown`, `Não Informado`, `N/A`) viram nulo;
- o valor só prossegue se corresponder ao formato de um montante, com prefixo de moeda opcional (`USD`, `$`, `R$`), dígitos com separadores e sufixo multiplicador opcional. O que não casa é texto vazado e vira nulo;
- sufixos de escala são interpretados: `K` multiplica por mil, `M` por milhão, `B` por bilhão;
- conversão para `DECIMAL(18,2)` com `try_cast`;
- valores zerados ou negativos são tratados como ausência.

#### Por que a validação por formato, e não rejeição de valores com letras

A primeira hipótese de correção foi rejeitar qualquer valor contendo letras, que é a regra usada na coluna de popularidade. Aplicá-la aqui teria descartado todos os `USD 150000000` e `34.0M`, ou seja, dados perfeitamente válidos.

A diferença entre as duas colunas é que a monetária tem um vocabulário legítimo pequeno e conhecido (prefixos de moeda e sufixos de escala), enquanto a popularidade não tem nenhum. Validar contra o formato esperado preserva o legítimo e rejeita o resto.

#### Validação do resultado

A correção foi confirmada por conferência cruzada com valores públicos reais: os maiores orçamentos da base passaram a coincidir com os valores conhecidos de `Avatar: The Way of Water` (460 milhões), `Avengers: Endgame` (356 milhões), `Fast X` (340 milhões), `The Flash`, `Avengers: Infinity War` e `Justice League` (300 milhões cada). Se houvesse erro de escala remanescente, esses valores estariam todos deslocados em conjunto.

#### Limitações conhecidas

Quatro registros apresentam orçamento implausível (`Enea`, as duas entradas de `Red Dead Redemption 2` e `Lost in the Stars`). São registros corrompidos ou fictícios na origem, com provável mistura de moedas em um deles. Nenhum teto de valor foi aplicado, porque qualquer limite que os eliminasse descartaria também produções legítimas de alto orçamento.

---

### 4. Column Shift nas métricas de engajamento

Dois bugs nesta tabela só apareceram ao validar o resultado da consulta de filmes mais populares, que retornava obras desconhecidas com valores absurdos no topo do ranking.

**Sinopse vazada na coluna de popularidade.** A limpeza por remoção de caracteres não numéricos transformava frases em números: o texto `"...in early 90s and it's growth in 2010s"` produzia `902010`, um valor sintaticamente válido que dominava o ranking. A correção foi rejeitar qualquer valor contendo letras, em vez de tentar extrair dígitos dele.

**Ano de lançamento vazado como popularidade.** Registros apresentavam popularidade exatamente igual a `2020` ou `2019`. Como a métrica real do TMDB é sempre fracionária, inteiros situados entre 1870 e 2030 são tratados como ano deslocado e invalidados.

**Decisão de projeto.** Notas fora da escala de 0 a 10 são **descartadas, não reescaladas**. Um valor como `85` poderia ser `8.5` multiplicado por erro de escala, mas a inferência não é segura: o enunciado pede desconsiderar, e reescalar introduziria dado fabricado.

**Limitação.** A regra do inteiro em faixa de ano anula, por construção, qualquer popularidade legítima que seja um inteiro exato entre 1870 e 2030. É estatisticamente improvável, mas é um falso positivo possível. A faixa é arbitrária: cobre desde o início do cinema até uma margem futura.

---

### 5. Domínio fechado contra domínio aberto: duas estratégias

As colunas de gêneros e de nomes de pessoas sofreram exatamente a mesma corrupção, mas exigiram abordagens opostas. O critério de escolha foi a natureza do domínio.

#### Gêneros: whitelist

A inspeção dos valores distintos revelou corrupção severa. Sinopses inteiras, idiomas (`English`, `German`), nomes de pessoas (`Jon Binkowski`), produtoras (`SVF Entertainment`) e números (`0.6`) haviam vazado para a coluna de gêneros.

Tentar enumerar padrões de lixo é inviável, porque a variedade é ilimitada. Como o conjunto de gêneros é **fechado e conhecido**, inverteu-se a lógica: em vez de remover o que é inválido, mantém-se apenas o que consta do catálogo TMDB/IMDb.

A whitelist resolve, como efeito colateral, a padronização de capitalização. Só passam valores com a grafia exata da lista, o que elimina a possibilidade de o mesmo gênero ser contabilizado duas vezes por diferença de caixa.

**Limitações.** Linhas em que os gêneros ficaram grudados à sinopse sem separador perdem seus gêneros. Recuperá-los exigiria busca por ocorrência dentro de texto livre, o que produziria falsos positivos (uma sinopse que mencione "war" seria classificada no gênero *War*). Optou-se pela perda consciente em favor de um catálogo confiável. Além disso, um gênero legítimo ausente da lista seria descartado silenciosamente; a lista foi conferida contra os valores distintos presentes na base.

#### Pessoas e produtoras: filtros heurísticos

Nomes de pessoas e de produtoras formam um domínio **aberto e ilimitado**, então não há lista de referência contra a qual validar. A alternativa foi inferir, a partir de características estruturais, o que não pode ser um nome:

| Regra | Justificativa |
|---|---|
| Descarta valor puramente numérico | Valor deslocado de outra coluna |
| Exige ao menos uma letra | Elimina pontuação e símbolos soltos |
| Exige início com letra maiúscula | Fragmentos de sinopse tipicamente começam em minúscula |
| Limita a 80 caracteres e 10 palavras | Nome é rótulo, não frase |
| Descarta valores com `?` ou `!` | Pontuação de frase não ocorre em nome próprio nem em razão social |

Os limites de tamanho foram calibrados empiricamente. Com o corte inicial em 50 caracteres e 6 palavras, nomes institucionais legítimos como *Helsinki Metropolia University of Applied Sciences* e *National Film Development Corporation of India* estavam sendo eliminados. Os valores atuais preservam esses casos.

**Limitações.**

- **Fragmentos de texto sobreviventes.** A inspeção dos nomes mais longos revelou casos como *"A Feature Documentary About Callie Truelove"* e *"Bright Lights. Big Dreams. No Experience."*, trechos de sinopse ou slogan que satisfazem todos os critérios estruturais. Endurecer os limites eliminaria junto nomes legítimos longos.
- **Entidades na coluna errada.** Valores como *"Executive Committee Of Kumamoto Film Production"* são organizações que aparecem nas colunas de pessoas físicas na origem. Como o tipo é derivado da coluna de origem e não do conteúdo, esses registros são classificados conforme o campo em que estão.
- **Nomes concatenados.** Em *"Francis Indio Disla Ferreirajuan Jose Namnun"* faltou o separador na origem, unindo duas pessoas num único valor. Não há delimitador a recuperar.
- **Perda de capitalização interna.** O `initcap` rebaixa siglas e nomes compostos (`(TMG)` vira `(Tmg)`, `Baden-Württemberg` vira `Baden-württemberg`). É um custo assumido: o enunciado pede padronização de capitalização, e sem ela o mesmo nome em grafias diferentes seria contado como entidades distintas, comprometendo a contagem de participações por ator.

---

### 6. A escolha da cotação do dólar

#### A decisão

Todos os valores em reais são calculados com uma **taxa única**: a cotação mais recente disponível na tabela de câmbio. O mesmo multiplicador é aplicado a um filme de 1970 e a um de 2024.

#### Por que ela foi tomada

A alternativa correta do ponto de vista contábil seria converter cada filme pela cotação vigente na sua data de lançamento. Isso não foi viável no escopo do projeto: a ingestão da API do Banco Central consulta apenas os últimos dias corridos, enquanto o catálogo cobre várias décadas. Não há série histórica carregada com a qual cruzar.

Também foi avaliada e descartada a ideia de estimar as cotações ausentes a partir da variância das semanas anteriores. O enunciado pede preenchimento por forward fill, que é determinístico e auditável (o câmbio de sábado é o de sexta, não uma estimativa), enquanto uma projeção estatística sobre uma janela de poucos dias produziria números com aparência de precisão e nenhuma base real.

#### O que isso significa na prática

Os valores em BRL não representam "quanto o filme faturou em reais na época", e sim "quanto esse montante em dólares vale em reais hoje". São grandezas diferentes, e a distinção precisa estar clara para quem consome os dados, caso contrário comparações históricas em reais levam a conclusões erradas.

Há ainda um efeito colateral: como a taxa é recarregada a cada execução do pipeline, **os valores em reais mudam entre rodadas**. A resposta da pergunta sobre receita total em R$ é sensível à data de processamento. Os valores em dólar permanecem estáveis.

#### Melhorias

- **Carregar a série histórica de câmbio.** A API do Banco Central aceita períodos amplos, não apenas os últimos dias. Ampliando a janela de ingestão e fazendo a junção pela data de lançamento, a conversão passaria a ser contabilmente correta. É factível: foi uma limitação de escopo, não uma barreira técnica.
- **Registrar a cotação utilizada na própria tabela.** Uma coluna com a taxa e a data de referência tornaria cada valor em reais reproduzível e auditável, resolvendo a variação silenciosa entre execuções.
- **Separar as duas leituras** em colunas distintas: valor convertido pela cotação de lançamento (visão histórica) e pela cotação atual (visão de poder de compra hoje). São perguntas de negócio diferentes e merecem campos diferentes.

---

### 7. Decisões pontuais

#### Remoção de linhas sem dado financeiro

Registros em que orçamento e receita eram ambos nulos após o tratamento foram removidos da `tb_financeiro_filmes`.

A decisão é segura porque a tabela fato monta o grão a partir da lista de filmes e faz `LEFT JOIN` com a financeira. Um filme sem linha correspondente aparece com métricas nulas, exatamente o mesmo resultado que teria com a linha vazia presente. Nenhum número muda.

O ganho é de usabilidade para quem consome os dados. A tabela contém apenas registros com informação financeira real, evitando que o analista precise filtrar nulos a cada consulta ou conclua erroneamente que a cobertura de dados é maior do que é.

**Melhoria:** em vez de descartar, mover essas linhas para uma tabela de registros sem dado financeiro. Preserva a rastreabilidade, permitindo responder "quantos filmes não têm informação financeira e quais são", sem poluir a tabela de consumo.

#### Restrição do idioma a dois caracteres

O campo `original_language` segue o padrão ISO 639-1, que usa exatamente duas letras minúsculas (`en`, `pt`, `ja`). Diferente da sinopse, aqui existe um formato rígido, o que permite validação determinística em vez de heurística.

Qualquer valor que não case com esse padrão é, por definição, conteúdo deslocado de outra coluna. Foi esse teste que revelou a presença de frases inteiras no campo, e ele virou o sinal primário para marcar a linha inteira como corrompida.

**Melhoria:** validar contra a lista real de códigos ISO 639-1, e não apenas contra o formato. Um valor como `zz` tem duas letras e passa na verificação atual, mas não corresponde a idioma nenhum. São cerca de 180 códigos, que caberiam numa lista de referência no mesmo modelo usado para os gêneros.

#### `titulo` e `titulo_original`

São campos distintos na origem, com propósitos diferentes:

| Campo | Origem | Conteúdo |
|---|---|---|
| `titulo` | `title` | Título de exibição, normalmente na língua do mercado de distribuição |
| `titulo_original` | `original_title` | Título no idioma original da produção |

Para um filme japonês, o primeiro pode trazer o título internacional em inglês e o segundo o título em japonês. Para uma produção nacional, os dois costumam coincidir.

**Tratamento.** Ambos recebem o mesmo processamento: remoção de espaços nas bordas e limpeza do resíduo de escape do CSV. Nenhum dos dois é convertido para maiúsculas, decisão tomada após reverter uma versão anterior que aplicava `upper()` e prejudicava a legibilidade no documento gerado para o time de IA. Nenhum dos dois é anulado, mesmo em linhas marcadas como corrompidas, por serem a única identificação legível do registro.

**Limitação relevante.** Apenas `titulo` é usado na construção do mapa de identidade. É uma escolha questionável, porque `titulo_original` é por definição mais estável entre registros duplicados, justamente por não variar conforme o mercado de distribuição. Aproveitá-lo teria resolvido o caso `Duro de Atuar 2`. É a melhoria de maior retorno listada neste documento.

---

## Tratamento por tabela

### `tb_cotacao_dolar`

Origem: `bronze.tb_cotacao_dolar`, alimentada pela API PTAX do Banco Central.

1. Conversão do timestamp para data, com agregação de um único valor por dia, porque a API pode retornar mais de um registro para a mesma data.
2. Geração de um calendário contínuo entre a menor e a maior data do período, com `sequence()` e `explode()`.
3. `LEFT JOIN` do calendário com as cotações reais, deixando nulos os dias sem pregão.
4. Forward fill com `last(..., ignorenulls=True)` sobre janela ordenada por data, com `rowsBetween(unboundedPreceding, 0)`, de modo que cada dia sem cotação herde o valor do último dia útil disponível.

### `tb_info_filmes`

Origem: `bronze.tb_movies_info`.

1. Deduplicação de reingestão com `row_number()` particionado por identificador e ordenado por `ingestion_datetime` decrescente.
2. Tradução para o identificador canônico, seguida de um segundo `row_number()` que colapsa os registros do mesmo filme.
3. Normalização e tradução do status: remoção de hífens e underscores, colapso de espaços, padronização de caixa e mapeamento para português. Valores não mapeáveis recebem `"Não Informado"`.
4. Conversão de data multi-formato com `coalesce` sobre várias tentativas de `try_to_date`. Só vira nulo o que não casa com nenhum formato.
5. Coluna derivada `ano_lancamento`, extraída da data já convertida.
6. Tipagem de `duracao_minutos` com `try_cast`; valores zerados ou negativos são tratados como ausência, não como duração real.
7. Validação de `idioma_original` contra o padrão ISO 639-1.
8. Limpeza do resíduo de escape e invalidação da sinopse nas linhas identificadas como corrompidas.

**Decisão.** Registros com status `"Não Informado"` são mantidos. O enunciado determina padronizar os não mapeáveis com esse rótulo, o que implica preservá-los, e eles não contaminam as métricas porque a tabela fato filtra apenas filmes lançados.

### `tb_financeiro_filmes`

Origem: `bronze.tb_movies_financials` e `silver.tb_cotacao_dolar`.

1. Deduplicação de reingestão e tradução para o identificador canônico.
2. Descarte de registros com identificador não numérico. Em linhas gravemente desalinhadas o próprio campo de identificação recebeu texto, e um identificador que não é número jamais casaria com a dimensão de filmes.
3. Higienização monetária com interpretação de sufixos de escala, descrita na seção 3.
4. Conversão para BRL aplicando a cotação mais recente disponível.
5. Colunas derivadas de lucro (dólar e real) e margem percentual, protegida contra divisão por zero e por nulo.
6. Remoção de linhas sem nenhum dado financeiro.
7. Deduplicação final por filme, com ordenação que prioriza a linha com valores preenchidos, evitando que a consolidação descarte dados bons ficando com uma duplicata vazia.

### `tb_metricas_engajamento`

Origem: `bronze.tb_movies_metrics`.

1. Deduplicação de reingestão, tradução para o identificador canônico e deduplicação final por filme, garantindo o grão exigido pela tabela fato.
2. Popularidade em quatro etapas: invalidação de valores contendo letras antes de qualquer limpeza; normalização de separador decimal e de milhar; remoção de caracteres residuais e conversão com `try_cast`; invalidação de negativos e de inteiros em faixa plausível de ano.
3. Conversão segura das demais métricas com `try_cast`, de modo que textos fora de contexto virem nulo sem interromper a execução.
4. Regras de limite de negócio: notas fora do intervalo de 0 a 10 são desconsideradas e contagens de votos negativas são invalidadas.

### `tb_avaliacoes_usuarios`

Origem: `bronze.tb_movies_reviews`.

1. Tradução para o identificador canônico antes da deduplicação, para que avaliações registradas sob identificadores duplicados do mesmo filme sejam consolidadas.
2. Remoção de duplicatas integrais, o que também resolve as cópias de reingestão, já que linhas reingeridas são idênticas.
3. Validação da nota com `try_cast` e invalidação de valores fora da escala de 0 a 10.
4. Padronização de comentários vazios para `"Sem comentário"`, com `coalesce` cobrindo também o caso de nulo puro.

**Decisão.** Nota fora da escala invalida apenas o valor, não a linha. A avaliação existiu e o comentário permanece útil.

**Efeito na Gold.** A `dim_reviews` agrega esta tabela por filme. A média usa `avg()`, que ignora nulos automaticamente, de modo que notas invalidadas não puxam o resultado para baixo.

**Limitações.** A deduplicação é por igualdade exata, então duas avaliações do mesmo usuário com diferença mínima de texto são mantidas como registros distintos. Não há validação da identidade do avaliador.

### `tb_generos` e `tb_pessoas_empresas`

Ambas originadas de `bronze.tb_credits_and_tags`. O tratamento e o racional estão descritos na seção 5.

A `tb_pessoas_empresas` consolida quatro tipos de entidade numa dimensão unificada:

| Coluna de origem | `tipo_entidade` |
|---|---|
| `cast` | Ator |
| `directors` | Diretor |
| `writers` | Roteirista |
| `production_companies` | Produtora |

---

## Camada Gold: modelo dimensional

### Surrogate keys

Geradas por hash SHA-256 da chave natural, convertido para `BIGINT`. A escolha do hash sobre `monotonically_increasing_id()` ou `row_number()` se deve ao **determinismo**: a mesma chave natural produz sempre o mesmo valor, o que permite que fato e dimensão gerem chaves compatíveis sem depender de join nem de ordem de execução.

A `dim_people` usa hash composto de `nome_pessoa` e `tipo_pessoa`, não apenas do nome. A mesma pessoa pode atuar como Diretor num filme e Roteirista em outro, e o enunciado define `tipo_pessoa` como atributo da dimensão, portanto cada combinação nome e papel é uma linha distinta. Como consequência, a `bridge_movie_person` faz junção composta pelas duas colunas.

### Constraints declaradas

As chaves primárias das dimensões e a chave estrangeira da tabela fato foram declaradas no Unity Catalog. São informativas e não bloqueiam escrita, mas documentam o modelo dimensional e permitem que o otimizador as considere no planejamento das consultas.

O bloco de constraints é idempotente (remove antes de recriar), porque `ADD CONSTRAINT` falharia na segunda execução do Job agendado.

### Tabela de contexto para RAG

A coluna `llm_context_document` é montada por concatenação, e cada campo passa por `coalesce` com um texto de substituição **antes** de entrar no `concat`.

Isso neutraliza o comportamento do `concat`, que retorna nulo se qualquer argumento for nulo. Sem essa proteção, um único campo ausente faria o filme inteiro desaparecer da base vetorial de forma silenciosa. Com ela, a contagem de registros da tabela de contexto é idêntica à da `dim_movies`.

---

## Avisos do otimizador do Databricks

Durante a execução do Job, o advisor de performance sinalizou três agregações consideradas potencialmente redundantes. Os três foram analisados individualmente e nenhum foi aplicado.

O advisor é heurístico: detecta padrões de agrupamento no plano de execução e sugere removê-los quando a chave agregada parece já ser única. Ele não conhece as garantias de negócio da origem, e em um dos casos a sugestão produziria erro grave.

**`tb_cotacao_dolar`, agregação por `data`.** O `groupBy("data")` que precede o forward fill é o que garante uma cotação por dia. A API pode retornar mais de um registro para a mesma data, e a ingestão em modo `append` acrescenta uma nova leitura a cada execução. Sem essa agregação, a junção com o calendário contínuo multiplicaria as linhas e a janela do forward fill passaria a operar sobre uma série com datas repetidas. A agregação não é redundante: é justamente ela que estabelece a unicidade que o otimizador presume já existir.

**`dim_movies`, deduplicação por filme.** Aqui o aviso está tecnicamente correto, porque a `tb_info_filmes` já garante um registro por filme através de duas passadas de deduplicação. Optou-se por manter a verificação como salvaguarda: a chave primária depende dessa unicidade, e a origem já se mostrou capaz de produzir duplicações por caminhos inesperados ao longo do projeto. O custo é um shuffle de poucos segundos; o custo de a garantia falhar silenciosamente seria uma dimensão com chave duplicada contaminando todos os joins da Gold.

**`dim_people`, deduplicação por nome e tipo.** Esta sugestão está incorreta e aplicá-la quebraria a tabela. A origem tem grão de uma linha **por participação**, não por pessoa: um ator que atuou em cinquenta filmes aparece cinquenta vezes. O `distinct()` é exatamente o que realiza a transformação de grão. Removê-lo produziria uma dimensão com uma linha por participação, violando a chave primária declarada e inflando toda contagem que passe por ela, incluindo a consulta de participações por ator.

**Conclusão.** Dos três avisos, um seria neutro, um comprometeria a série de câmbio e um quebraria a dimensão de pessoas. A recomendação de declarar constraints, presente nos três, foi acatada.

O episódio ilustra um ponto de método: sugestões automáticas de otimização avaliam o plano de execução, não a semântica do dado. Aplicá-las sem verificar a garantia de unicidade na origem troca segundos de processamento por defeitos silenciosos de correção.

---

## Melhorias futuras consolidadas

Em ordem de impacto:

1. **Configurar corretamente o parsing do CSV na Bronze** (`multiLine`, `escape`, `columnNameOfCorruptRecord`). Elimina a causa raiz de problemas hoje tratados individualmente em cinco tabelas.
2. **Usar `titulo_original` no mapa de identidade.** Resolveria a classe de títulos localizados sem nenhuma heurística nova.
3. **Carregar a série histórica de câmbio** e converter cada filme pela cotação da sua data de lançamento.
4. **Substituir as passadas de `min()` por componentes conexos reais** (GraphFrames) na consolidação de identidade.
5. **Tornar a chave canônica estável entre cargas**, derivando-a de hash do conteúdo ou mantendo uma tabela de identidades que nunca reatribui membros existentes.
6. **Criar tabelas de quarentena** para registros não resolvidos, em vez de descartá-los ou fundi-los silenciosamente.
7. **Validar `idioma_original` contra a lista real de códigos ISO 639-1**, não apenas contra o formato de duas letras.
8. **Registrar a cotação utilizada** como coluna da tabela financeira, tornando os valores em reais reproduzíveis.
9. **Adicionar testes automatizados de qualidade** (unicidade de grão, integridade referencial, faixas de negócio) executados como task do Job, transformando em falha explícita o que hoje depende de inspeção manual.

---

## Orquestração e execução

O pipeline é orquestrado por um Databricks Workflow com três tasks encadeadas por dependência explícita:

```
to_Bronze  →  to_Silver  →  to_Gold
```

O Job possui agendamento configurado, simulando uma rotina de atualização em produção. A definição exportada está em `job.yaml`, e o print da execução bem-sucedida evidencia as dependências entre as tarefas.

**Observação sobre execuções repetidas.** A camada Bronze grava em modo `append`, portanto cada execução acrescenta uma nova carga às tabelas brutas. Isso é intencional e está tratado: todas as tabelas Silver deduplicam por `ingestion_datetime`, mantendo apenas o registro mais recente de cada filme. A contagem de linhas da Bronze cresce a cada rodada; os resultados analíticos permanecem estáveis, com exceção dos valores em reais, que acompanham a cotação vigente.
