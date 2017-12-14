import pandas as pd


class ColumnPreProcessor():
    def __init__(self, col_name, to_col_name=None):
        self.col_name = col_name
        self.to_col_name = to_col_name
        self.filters = []
        self.transforms = []

    def add_filter(self, filter_str):
        self.filters.append(lambda x: eval(filter_str))

    def add_transformer(self, transform_str):
        self.transforms.append(lambda x: eval(transform_str))

    def get_transform_fn(self):
        if(len(self.transforms) > 0):
            def fn(cell_val):
                for transform in self.transforms:
                    cell_val = transform(cell_val)
                    return cell_val
        else:
            fn = None
        
        return fn

    def get_filter_fn(self):
        def fn(cell_val):
            remvoe_flag = False
            if(len(self.filters) > 0):
                for filter in self.filters:
                    pass
        return fn


class CompareConfig():
    column_suffixes = ('_src', '_target')
    _compare_value_column = 'value'
    src_processors = []
    target_processors = []

    def __init__(self,
                 compare_column='value'):
        self._compare_value_column = compare_column
        # if(not (src and target)):
        #     raise ValueError('src and target file path should be specified.')

    def append_processor(self,
                         from_column,
                         to_column=None,
                         transforms=[],
                         filters=[],
                         src_flag=True):
        if to_column is None:
            to_column = from_column

        # build processor
        processor = ColumnPreProcessor(from_column, to_column)
        if(len(transforms) > 0):
            for t in transforms:
                processor.add_transformer(t)

        if(len(filters) > 0):
            for f in filters:
                processor.add_filter(f)

        # assignment
        if(src_flag):
            self.src_processors.append(processor)
        else:
            self.target_processors.append(processor)

    def get_join_columns(self):
        def _extract_column_name(processor):
            return processor.to_col_name

        src_columns = set(map(_extract_column_name, self.src_processors))
        target_columns = set(map(_extract_column_name, self.target_processors))

        if(src_columns == set() or target_columns == set()):
            return list(src_columns.union(target_columns))

        if(src_columns == target_columns):
            return list(src_columns)

        raise ValueError("src column {} and target column {} are different"
                         .format(src_columns, target_columns))

    def get_compare_columns(self):
        select_keys_list = []
        for k in self.column_suffixes:
            select_keys_list.append(self._compare_value_column + k)
        return select_keys_list

    def get_result_value_column(self):
        return self._compare_value_column + "_diff"

    def get_result_columns(self):
        result_columns = []
        result_columns.extend(config.get_join_columns())
        result_columns.extend(config.get_compare_columns())
        result_columns.extend([config.get_result_value_column()])
        return result_columns

def handle_column_proprocessors(df, processors):
    if(len(processors) > 0):
        for p in processors:
            handle_column_proprocessor(df, p)


def handle_column_proprocessor(df, processor):
    trans_fn = processor.get_transform_fn()
    tmp = df[processor.col_name]
    if(trans_fn):
        df[processor.to_col_name] = tmp.apply(
            lambda x: trans_fn(x))
    else:
        df[processor.to_col_name] = tmp


def compare_column_value(row):
    print(type(row))
    src = row[config.get_compare_columns()[0]]
    target = row[config.get_compare_columns()[1]]

    if(pd.isna(src)):
        return "missing_src"
    if(pd.isna(target)):
        return "missing_target"
    if(src == target):
        return "equal"
    if(src != target):
        return "not equal"


# renovation
src_df2 = pd.read_csv('./testdata.csv')
target_df2 = pd.read_csv('./testdata_target.csv')

config = CompareConfig('value')
config.append_processor('A1',
                        to_column='A',
                        transforms=["x.strip('prefix_')"],
                        src_flag=False)
config.append_processor('B2', to_column='B', src_flag=False)

handle_column_proprocessors(target_df2, config.target_processors)
handle_column_proprocessors(src_df2, config.src_processors)

merged_df2 = pd.merge(src_df2, target_df2,
                      on=config.get_join_columns(),
                      suffixes=config.column_suffixes,
                      how="outer")

# merged_df2[config.get_result_value_column()]\
#     = merged_df2[config.get_compare_columns()[0]] == \
#     merged_df2[config.get_compare_columns()[1]]

merged_df2[config.get_result_value_column()] = merged_df2.apply(
    compare_column_value, axis=1)
merged_df2[config.get_result_columns()]


# def draft_action():

#     def get_select_keys():
#         return ['A', 'B', 'value_src', 'value_target', 'value_diff']

#     target_preprocessor = ColumnPreProcessor("A1", "A")
#     target_preprocessor.add_filter("not x.startswith('prefix_')")
#     target_preprocessor.add_transformer("x.strip('prefix_')")

#     target_preprocessor2 = ColumnPreProcessor("B2", "B")

#     src_df = pd.read_csv('./testdata.csv')
#     target_df = pd.read_csv('./testdata_target.csv')

#     handle_column_proprocessor(target_df, target_preprocessor)
#     handle_column_proprocessor(target_df, target_preprocessor2)

#     merged_df = pd.merge(src_df,
#                          target_df, on=['A', 'B'],
#                          suffixes=('_src', '_target'))

#     merged_df['value_diff']\
#         = merged_df['value_src'] == merged_df['value_target']

#     select_keys = get_select_keys()
#     merged_df[select_keys]


selected = merged_df2[['value_src', 'value_target', 'value_diff']]
selected['value_name'] = 'attribute123'

selected.groupby(['value_name', 'value_diff']).count()
