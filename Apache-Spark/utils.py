from pyspark.sql import functions as f # import da biblioteca functions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator # importa classe MulticlassClassificationEvaluator

# cria função que vai receber os dados para serem avaliados
def calcula_mostra_metricas(modelo_lr, df_transform_modelo, normalize=False, percentage=True):
# possibilidade de output para criação da matriz de confusão
  tp = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 1) & (f.col('prediction') == 1)).count()
  tn = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 0) & (f.col('prediction') == 0)).count()
  fp = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 0) & (f.col('prediction') == 1)).count()
  fn = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 1) & (f.col('prediction') == 0)).count()

  valorP = 1
  valorN = 1

  if normalize:
    valorP = tp + fn
    valorN = fp + tn

  if percentage and normalize:
    valorP = valorP / 100
    valorN = valorN / 100

  # ‘s’ será a string de retorno
  # ela vai coletar e montar a matriz de confusão
  # e também os valores de acurácia, precisão, recall e F1-score
  s = ''

  # construção da string da matriz de confusão  
  s += ' '*20 + 'Previsto\n'
  s += ' '*15 +  'Churn' + ' '*5 + 'Não-Churn\n'
  s += ' '*4 + 'Churn' + ' '*6 +  str(int(tp/valorP)) + ' '*7 + str(int(fn/valorP)) + '\n'
  s += 'Real\n'
  s += ' '*4 + 'Não-Churn' + ' '*2 + str(int(fp/valorN)) +  ' '*7 + str(int(tn/valorN))  + '\n'
  s += '\n'

  # coleta o resumo das métricas com summary
  resumo_lr_treino = modelo_lr.summary

  # adiciona os valores de cada métrica a string de retorno
  s += f'Acurácia: {resumo_lr_treino.accuracy}\n'
  s += f'Precisão: {resumo_lr_treino.precisionByLabel[1]}\n'
  s += f'Recall: {resumo_lr_treino.recallByLabel[1]}\n'
  s += f'F1: {resumo_lr_treino.fMeasureByLabel()[1]}\n'

  return s

# cria função que vai receber os dados para serem avaliados
def calcula_mostra_metricas_evaluate(df_transform_modelo, normalize=False, percentage=True):
# possibilidade de output para criação da matriz de confusão
  tp = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 1) & (f.col('prediction') == 1)).count()
  tn = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 0) & (f.col('prediction') == 0)).count()
  fp = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 0) & (f.col('prediction') == 1)).count()
  fn = df_transform_modelo.select('label', 'prediction').where((f.col('label') == 1) & (f.col('prediction') == 0)).count()

  valorP = 1
  valorN = 1

  if normalize:
    valorP = tp + fn
    valorN = fp + tn

  if percentage and normalize:
    valorP = valorP / 100
    valorN = valorN / 100

  # ‘s’ será a string de retorno
  # ela vai coletar e montar a matriz de confusão
  # e também os valores de acurácia, precisão, recall e F1-score
  s = ''

  # construção da string da matriz de confusão  
  s += ' '*20 + 'Previsto\n'
  s += ' '*15 +  'Churn' + ' '*5 + 'Não-Churn\n'
  s += ' '*4 + 'Churn' + ' '*6 +  str(int(tp/valorP)) + ' '*7 + str(int(fn/valorP)) + '\n'
  s += 'Real\n'
  s += ' '*4 + 'Não-Churn' + ' '*2 + str(int(fp/valorN)) +  ' '*7 + str(int(tn/valorN))  + '\n'
  s += '\n'

  # adiciona os valores de cada métrica a string de retorno com MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator()

  s += f'Acurácia: {evaluator.evaluate(df_transform_modelo, {evaluator.metricName: "accuracy"})}\n'
  s += f'Precisão: {evaluator.evaluate(df_transform_modelo, {evaluator.metricName: "precisionByLabel", evaluator.metricLabel: 1})}\n'
  s += f'Recall: {evaluator.evaluate(df_transform_modelo, {evaluator.metricName: "recallByLabel", evaluator.metricLabel: 1})}\n'
  s += f'F1: {evaluator.evaluate(df_transform_modelo, {evaluator.metricName: "fMeasureByLabel", evaluator.metricLabel: 1})}\n'

  return s
