# Rocket-visagio-eng_dadosConsolidação de filmes duplicados sob ids distintos
O problema

Além das duplicatas de reingestão (mesmo id gravado várias vezes na Bronze pelo modo append, tratadas com row_number() sobre ingestion_datetime), a base contém o mesmo filme cadastrado sob ids naturais diferentes.

O caso mais extremo encontrado: ~60 registros do filme "Die Hart 2", distribuídos em 9 variações de título e cerca de 10 datas de lançamento distintas.

Isso inflava todas as métricas do modelo: contagem de participações por ator, receita total, volume por gênero e número de linhas na dim_movies.

Por que a regra do enunciado não cobre

O enunciado pede unicidade por filme mantendo "a versão mais recente com base na data de ingestão" — regra que resolve o mesmo id reingerido. Ela não alcança registros com ids diferentes que representam a mesma obra, porque para o pipeline são filmes distintos.

Dificuldades encontradas

As variações não seguem um padrão único, e cada tentativa de solução revelou um novo caso de borda:

1. Agrupar por título normalizado (minúsculas, sem pontuação) — resolveu caixa e separadores (DIE HART 2: DIE HARTER, Die Hart 2 - Die Harter, Die Hart 2 : Die Harter), mas fundiu filmes homônimos: 8 obras diferentes chamadas Silence (2016 a 2021), 7 chamadas The Outsider, além de Inferno, Sundown, Collide e Genius. Títulos curtos são reutilizados com frequência no cinema.

2. Acrescentar a data de lançamento à chave — separou os homônimos corretamente. Exigiu parsear a data antes da comparação, pois a origem mistura formatos: 09-10-2016 e 2016-09-10 são o mesmo dia escrito de formas diferentes. Porém, o título truncado (Die Hart 2 sem o subtítulo) e o título localizado (Duro de Atuar 2, versão em português) continuaram fora do grupo.

3. Usar a sinopse como segunda chave — a sinopse é longa e distintiva, então liga registros cujo título não tem nada em comum. Mas fundiu obras seriadas que compartilham o mesmo texto: Ah, Wilderness: Part 1 com Part 2, e Yuki Yuna Is a Hero: Washio Sumi Chapter 1/2/3.

4. Guarda por assinatura numérica do título — bloqueou a fusão das partes (1 ≠ 2), mas, sendo uma comparação de igualdade estrita, passou a separar variações legítimas do mesmo filme: Die Hart: Die Harter (sem dígito) ficava fora do grupo de Die Hart 2: Die Harter (dígito "2"), mesmo com sinopse idêntica.

5. Metade dos registros não tem sinopse — dos ~60 registros do Die Hart 2, cerca de 30 têm overview nulo, o que torna qualquer regra baseada em sinopse insuficiente sozinha.

Solução adotada

Uma tabela de mapeamento silver.tb_mapa_id_filme (id_filme_original → id_filme_canonico), construída em quatro passadas de evidência crescente. Todas as tabelas Silver traduzem seu id por esse mapa logo após a leitura da Bronze, antes de qualquer agregação — assim os dados dos ids duplicados se consolidam sob o mesmo filme e a camada Gold funciona sem alteração.

Passada	Chave de agrupamento	Objetivo
1	título normalizado + data parseada	Une variações de caixa e pontuação, sem fundir homônimos
2a	sinopse (≥ 80 caracteres) + assinatura numérica	Une títulos diferentes com mesma sinopse, preservando obras seriadas
2b	sinopse, apenas para títulos sem dígito	Permite que variações sem numeral (Die Hart: Die Harter) entrem no grupo, sem afetar Part 1 vs Part 2 (ambos têm dígito)
3	prefixo de 2 palavras + ano + assinatura numérica, apenas para registros sem sinopse utilizável	Absorve os registros sem evidência textual própria; quem tem sinopse distinta não é tocado
4	propagação por canon_1	Transitividade: se a passada 1 considerou dois registros o mesmo filme, ambos terminam no mesmo grupo mesmo que só um tenha migrado nas passadas seguintes

Critérios de segurança aplicados: título ou data ausentes não participam do agrupamento (evita fundir todos os registros incompletos); a sinopse só é usada acima de 80 caracteres (texto curto não identifica uma obra); e o id canônico é o menor do grupo, garantindo resultado determinístico entre execuções.

Resultado

Os ~60 registros do Die Hart 2 passaram a compor um único filme, enquanto Die Hart (2023) permaneceu separado pelo ano e os filmes homônimos Silence continuaram distintos por terem sinopses próprias.

Limitações conhecidas
Duro de Atuar 2 (título localizado) permanece como filme separado: não compartilha título, prefixo nem sinopse com as demais variações, portanto não há evidência que o vincule.
Dois registros Silence de 2020, ambos sem sinopse, foram unificados. Compartilham título e ano e não possuem nenhum atributo que os distinga — a ambiguidade é irredutível com os dados disponíveis.
Alien: Covenant - Prologue: Advent e Epilogue: Advent são unificados quando compartilham data e sinopse, embora sejam peças distintas.

Em todos esses casos optou-se por não adicionar heurísticas mais agressivas, que resolveriam o caso isolado ao custo de reintroduzir fusões incorretas em outros pontos da base.
