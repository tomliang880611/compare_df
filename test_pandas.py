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


def handle_column_proprocessor(df, processor):
    trans_fn = processor.get_transform_fn()
    tmp = df[processor.col_name]
    if(trans_fn):
        df[processor.to_col_name] = tmp.apply(
            lambda x: trans_fn(x))
    else:
        df[processor.to_col_name] = tmp


target_preprocessor = ColumnPreProcessor("A1", "A")
target_preprocessor.add_filter("not x.startswith('prefix_')")
target_preprocessor.add_transformer("x.strip('prefix_')")

target_preprocessor2 = ColumnPreProcessor("B2", "B")

# data washing
src_df = pd.read_csv('./testdata.csv')
target_df = pd.read_csv('./testdata_target.csv')

handle_column_proprocessor(target_df, target_preprocessor)
handle_column_proprocessor(target_df, target_preprocessor2)

pd.merge(src_df, target_df, on=['A', 'B'])
