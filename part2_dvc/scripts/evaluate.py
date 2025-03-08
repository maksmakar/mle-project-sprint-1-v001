import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
import joblib
import json
import yaml
import os

# оценка качества модели
def evaluate_model():
    
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd) 
        
    with open('part2_dvc/models/fitted_model.pkl', 'rb') as fd:
        model = joblib.load(fd)
    data = pd.read_csv('part2_dvc/data/initial_data.csv') 

    target_col = params['target_col']  

    cv_strategy = StratifiedKFold(n_splits=params['n_splits'])
    cv_res = cross_validate(
        model,
        data,
        data[target_col],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics']
    )
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3) 

        # сохраните результата кросс-валидации в cv_res.json
    os.makedirs('part2_dvc/cv_results', exist_ok=True)
    with open('part2_dvc/cv_results/cv_res.json', 'w') as fp:
        json.dump(cv_res, fp)

if __name__ == '__main__':
	evaluate_model()