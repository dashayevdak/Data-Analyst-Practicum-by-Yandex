#!/usr/bin/python
# -*- coding: utf-8 -*-
 
# импорт библиотек
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
 
import plotly.graph_objs as go
 
from datetime import datetime
 
import pandas as pd
 
# задание данных для отрисовки
from sqlalchemy import create_engine
 
# подключение к бд zen
db_config = {'user': 'my_user',
             'pwd': 'my_user_password',
             'host': 'localhost',
             'port': 5432,
             'db': 'zen'}
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                            db_config['pwd'],
                                                            db_config['host'],
                                                            db_config['port'],
                                                            db_config['db']))
# получение данных
# датафрейм dash_visits
query = ''' SELECT *
            FROM dash_visits;
        '''
dash_visits = pd.io.sql.read_sql(query, con = engine)
dash_visits['dt'] = pd.to_datetime(dash_visits['dt'])
 
# датафрейм dash_engagement
query = ''' SELECT *
            FROM dash_engagement;
        '''
dash_engagement = pd.io.sql.read_sql(query, con = engine)
dash_engagement['dt'] = pd.to_datetime(dash_engagement['dt'])

note = '''
         Пользовательское взаимодействие с карточками статей в Яндекс.Дзене.
         Можно выбрать интервал даты, тему карточек, возрастную категорию пользователей для отображения на дашборде.
       '''
# задание лейаута
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, compress=False)
app.layout = html.Div(children=[  
   
    # формирование html
    html.H1(children = 'Дашборд для Яндекс.Дзен'),
 
    html.Br(),  
 
    # прописываем пояснение
    html.Label(note),
 
    html.Br(),  
 
    # Input
    html.Div([  
 
        html.Div([
 
            html.Div([
            # выбор даты
            html.Label('Дата:'),
            dcc.DatePickerRange(
                start_date = dash_visits['dt'].min(),
                end_date = dash_visits['dt'].max(),
                display_format = 'YYYY-MM-DD',
                id = 'dt_selector',      
                ),
            ]),
 
            html.Div([        
            # выбор возрастных категорий
            html.Label('Возрастные категории:'),
            dcc.Dropdown(
                options = [{'label': x, 'value': x} for x in dash_visits['age_segment'].unique()],
                value = dash_visits['age_segment'].unique().tolist(),
                multi = True,
                id = 'age-dropdown'
                ),    
            ]),
 
        ], className = 'six columns'),
 
        html.Div([    
            # выбор темы карточки
            html.Label('Темы карточек:'),
            dcc.Dropdown(
                options = [{'label': x, 'value': x} for x in dash_visits['item_topic'].unique()],
                value = dash_visits['item_topic'].unique().tolist(),
                multi = True,
                id = 'item-topic-dropdown'
            ),                
        ], className = 'six columns'),
 
       
    ], className = 'row'),
 
    html.Br(),      
 
    # Output
    html.Div([
        html.Div([
            # график истории событий по темам карточек
            html.Label('События по темам карточек:'),    
            dcc.Graph(
                style = {'height': '50vw'},
                id = 'history_absolute_visits'
            ),    
        ], className = 'six columns'),
    ]),
 
    html.Div([
 
        html.Div([
            # график разбивки событий по темам источников
            html.Label('События по темам источников:'),    
            dcc.Graph(
                style = {'height': '25vw'},
                id = 'pie_visits'
            ),  
        ], className = 'six columns'),            
 
        html.Div([
            # график средней глубины взаимодействия с карточками статей
            html.Label('Средняя глубина взаимодействия'),    
            dcc.Graph(
                style = {'height': '25vw'},
                id = 'engagement_graph'
            ),  
        ], className = 'six columns'),
       
    ], className = 'row'),  
   
 
    html.Br()    
 
])
 
# описание логики дашборда
@app.callback(
    [Output('history_absolute_visits', 'figure'),
     Output('pie_visits', 'figure'),
     Output('engagement_graph', 'figure'),
    ],
    [Input('dt_selector', 'start_date'),
     Input('dt_selector', 'end_date'),
     Input('item-topic-dropdown', 'value'),
     Input('age-dropdown', 'value'),
    ])
def update_figures(start_date, end_date, selected_item_topics, selected_ages):
 
    # применение фильтрации
    filtered_visits = dash_visits.query('item_topic.isin(@selected_item_topics) and \
                                        dt >= @start_date and dt <= @end_date and \
                                        age_segment.isin(@selected_ages)')
 
    filtered = dash_engagement.query('item_topic.isin(@selected_item_topics) and \
                                        dt >= @start_date and dt <= @end_date and \
                                        age_segment.isin(@selected_ages)')
 
    # создание вспомогательных таблиц
    visits_by_item_topic = (filtered_visits.groupby(['dt', 'item_topic'])
                            .agg({'visits': 'sum'})
                            .reset_index()
                      )
 
 
    visits_by_source_topic = (filtered_visits.groupby(['source_topic'])
                            .agg({'visits': 'sum'})
                            .reset_index()
                      )
 
 
    engagement_by_event = (filtered.groupby(['event'])
                            .agg({'unique_users': 'mean'})
                            .reset_index()
                            .rename(columns = {'unique_users': 'avg_unique_users'})
                            .sort_values(by = 'avg_unique_users', ascending = False)  
                      )
    engagement_by_event['avg_unique_users'] = round(engagement_by_event['avg_unique_users'])
    engagement_by_event['percent'] = round(engagement_by_event['avg_unique_users'] / engagement_by_event.loc[1,'avg_unique_users'] * 100)
    engagement_by_event['percent'] = engagement_by_event['percent'].astype('int')
 
    # график истории событий по темам карточек
    history_absolute_visits = []
    for item_topic in visits_by_item_topic['item_topic'].unique():
        history_absolute_visits += [go.Scatter(x = visits_by_item_topic.query('item_topic == @item_topic')['dt'],
                                   y = visits_by_item_topic.query('item_topic == @item_topic')['visits'],
                                   mode = 'lines',
                                   stackgroup = 'one',
                                   name = item_topic)]
 
    # график разбивки событий по темам источников €
    pie_visits = []
    pie_visits = [go.Pie(labels = visits_by_source_topic['source_topic'],
                             values = visits_by_source_topic['visits'])]
 
    # график средней глубины взаимодействия с карточками статей
    engagement_graph = []
    engagement_graph = [go.Bar(x = engagement_by_event['event'],
                            y = engagement_by_event['percent'])]
 
    # формирование результата для отображения
    return (
            {
                'data': history_absolute_visits,
                'layout': go.Layout(xaxis = {'title': 'Время'},
                                    yaxis = {'title': 'Количество событий'})
             },          
            {
                'data': pie_visits,
                'layout': go.Layout()
             },
            {
                'data': engagement_graph,
                'layout': go.Layout(xaxis = {'title': 'Взаимодействия с системой'},
                                    yaxis = {'title': 'Глубина взаимодействия (средний % от показов)'},
                                    barmode = 'stack',
                                    )
             },            
            )  
    
# основная часть исполняемого кода
if __name__ == '__main__':
    app.run_server(debug = True, host = '0.0.0.0',  port = 3005)

