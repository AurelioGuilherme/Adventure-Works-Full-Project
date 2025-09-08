import pandas as pd
import matplotlib.pyplot as plt
import warnings
import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import EarlyStopping, LearningRateMonitor
from pytorch_lightning.loggers import TensorBoardLogger
from pytorch_forecasting.models.temporal_fusion_transformer.tuning import optimize_hyperparameters
from pytorch_forecasting import TimeSeriesDataSet, TemporalFusionTransformer
from pytorch_forecasting.data import GroupNormalizer
from pytorch_forecasting.metrics import QuantileLoss
warnings.filterwarnings('ignore')


class ForecastingModel:
    def __init__(self, df, epochs):
        """
        Inicializa o modelo de previsão usando o TemporalFusionTransformer

        Args:
            df: DataFrame com colunas ['order_date_dt', 'name_store',
            'name_product', 'order_qty']
        """
        self.df = df.copy()
        self.epochs = epochs
        self.max_prediction_length = 3  # 3 meses de previsão
        self.max_encoder_length = 12    # 12 meses de histórico
        self.batch_size = 64
        self.num_workers = 0

        # Datasets
        self.full_dataset = None
        self.training_dataset = None
        self.validation_dataset = None
        self.test_dataset = None

        # Modelo
        self.model = None
        self.trainer = None

    def prepare_data(self):
        """Prepara os dados para o modelo"""
        # Criar time_idx
        self.df['time_idx'] = (self.df['order_date_dt'].dt.year * 12 +
                               self.df['order_date_dt'].dt.month)
        self.df['time_idx'] -= self.df['time_idx'].min()

        # Features baseadas no tempo
        self.df['month'] = self.df['order_date_dt'].dt.month
        self.df['year'] = self.df['order_date_dt'].dt.year
        self.df['day_of_year'] = self.df['order_date_dt'].dt.dayofyear

        # Garantir que order_qty seja float
        self.df['order_qty'] = self.df['order_qty'].astype(float)

        # Converter para categoria
        categorical_cols = ['name_store', 'name_product']
        for col in categorical_cols:
            self.df[col] = self.df[col].astype('category')
        return self

    def create_datasets(self):
        """Cria os datasets de treino, validação e teste"""

        # Definir pontos de corte
        max_time_idx = self.df["time_idx"].max()
        test_cutoff = max_time_idx - self.max_prediction_length
        validation_cutoff = test_cutoff - self.max_prediction_length

        print(f"Max time_idx: {max_time_idx}")
        print(f"Validation cutoff: {validation_cutoff}")
        print(f"Test cutoff: {test_cutoff}")

        # Dataset completo
        self.full_dataset = TimeSeriesDataSet(
            self.df,
            time_idx="time_idx",
            target="order_qty",
            group_ids=["name_product", "name_store"],
            max_encoder_length=self.max_encoder_length,
            max_prediction_length=self.max_prediction_length,
            min_encoder_length=self.max_encoder_length // 2,
            static_categoricals=["name_product", "name_store"],
            time_varying_known_reals=["month", "year", "day_of_year"],
            time_varying_unknown_reals=["order_qty"],
            target_normalizer=GroupNormalizer(
                groups=["name_product", "name_store"],
                transformation="softplus"
            ),
            add_relative_time_idx=True,
            add_target_scales=True,
            add_encoder_length=True,
            allow_missing_timesteps=True
        )

        # Dataset de treino
        self.training_dataset = TimeSeriesDataSet.from_dataset(
            self.full_dataset,
            self.df[self.df.time_idx <= validation_cutoff],
            stop_randomization=False
        )

        # Dataset de validação
        self.validation_dataset = TimeSeriesDataSet.from_dataset(
            self.full_dataset,
            self.df[self.df.time_idx <= test_cutoff],
            stop_randomization=True
        )

        # Dataset de teste
        self.test_dataset = TimeSeriesDataSet.from_dataset(
            self.full_dataset,
            self.df,
            stop_randomization=True
        )

        print(f"Training samples: {len(self.training_dataset)}")
        print(f"Validation samples: {len(self.validation_dataset)}")
        print(f"Test samples: {len(self.test_dataset)}")

        return self

    def create_dataloaders(self):
        """Cria os dataloaders"""
        self.train_dataloader = self.training_dataset.to_dataloader(
            train=True,
            batch_size=self.batch_size,
            num_workers=self.num_workers
        )

        self.val_dataloader = self.validation_dataset.to_dataloader(
            train=False,
            batch_size=self.batch_size * 2,
            num_workers=self.num_workers
        )

        self.test_dataloader = self.test_dataset.to_dataloader(
            train=False,
            batch_size=self.batch_size * 2,
            num_workers=self.num_workers
        )

        return self

    def create_model(self):
        """Cria o modelo TemporalFusionTransformer"""
        # Seed para reprodutibilidade
        pl.seed_everything(42)

        # Callbacks
        early_stop_callback = EarlyStopping(
            monitor="val_loss",
            min_delta=1e-4,
            patience=10,
            verbose=True,
            mode="min"
        )

        lr_logger = LearningRateMonitor()
        logger = TensorBoardLogger("lightning_logs")

        # Trainer
        self.trainer = pl.Trainer(
            max_epochs=self.epochs,
            accelerator="auto",
            devices=1 if torch.cuda.is_available() else None,
            enable_model_summary=True,
            gradient_clip_val=0.1,
            callbacks=[lr_logger, early_stop_callback],
            logger=logger,
            enable_progress_bar=True
        )

        # Modelo
        self.model = TemporalFusionTransformer.from_dataset(
            self.training_dataset,
            learning_rate=0.0003,
            hidden_size=8,
            attention_head_size=1,
            dropout=0.1,
            hidden_continuous_size=4,
            loss=QuantileLoss(),
            optimizer="ranger",
            reduce_on_plateau_patience=4,
        )

        return self

    def train_model(self):
        """Treina o modelo"""
        self.trainer.fit(
            self.model,
            train_dataloaders=self.train_dataloader,
            val_dataloaders=self.val_dataloader,
        )
        return self

    def make_predictions(self):
        """Faz previsões para validação, teste e para os 3 meses futuros"""
        # Previsões de validação
        self.val_predictions = self.model.predict(
            self.val_dataloader,
            mode="prediction",
            return_x=True
        )

        # Previsões de teste
        self.test_predictions = self.model.predict(
            self.test_dataloader,
            mode="prediction",
            return_x=True
        )

        # Previsões futuras (3 meses)
        self.future_predictions = self.model.predict(
            self.test_dataloader,
            mode="prediction",
            return_x=True
        )

        return self

    def get_prediction_dataframe(self, predictions, dataset_type="test"):
        """
        Converte previsões em DataFrame com informações de produto e loja

        Args:
            predictions: tupla (predictions_tensor, x_dict)
            dataset_type: tipo do dataset ("validation", "test", "future")
        """
        predictions_tensor = predictions[0]
        x_dict = predictions[1]

        # Extrair informações dos grupos
        groups = x_dict["groups"].cpu().numpy()

        # Mapear produtos e lojas
        product_mapping = {i: cat for i, cat in enumerate(
            self.training_dataset.categorical_encoders["name_product"].classes_
        )}
        store_mapping = {i: cat for i, cat in enumerate(
            self.training_dataset.categorical_encoders["name_store"].classes_
        )}
        results = []

        for i in range(len(predictions_tensor)):
            # Obter identificadores do grupo
            group = groups[i]
            product_id = group[0]
            store_id = group[1]

            product_name = product_mapping.get(product_id,
                                               f"Product_{product_id}")
            store_name = store_mapping.get(store_id, f"Store_{store_id}")

            # Previsões (mediana)
            pred_values = predictions_tensor[i].cpu().numpy()
            if len(pred_values.shape) > 1:
                pred_values = pred_values[:, pred_values.shape[1]//2]

            for j, pred_value in enumerate(pred_values):
                results.append({
                    'name_product': product_name,
                    'name_store': store_name,
                    'time_step': j + 1,
                    'prediction': pred_value,
                    'dataset_type': dataset_type
                })

        return pd.DataFrame(results)

    def create_comprehensive_plot(self, product=None, store=None):
        """
        Cria gráfico completo com dados reais e previsões

        Args:
            product: nome do produto específico (opcional)
            store: nome da loja específica (opcional)
        """
        # Filtrar dados se especificado
        plot_data = self.df.copy()
        if product:
            plot_data = plot_data[plot_data['name_product'] == product]
        if store:
            plot_data = plot_data[plot_data['name_store'] == store]

        if plot_data.empty:
            print("Nenhum dado encontrado para os filtros especificados")
            return

        # Agregar dados por time_idx
        historical_data = plot_data.groupby('time_idx')['order_qty']\
                                   .sum().reset_index()

        # Converter previsões para DataFrame
        val_df = self.get_prediction_dataframe(
            self.val_predictions, "validation"
        )
        test_df = self.get_prediction_dataframe(
            self.test_predictions, "test"
        )
        future_df = self.get_prediction_dataframe(
            self.future_predictions, "future"
        )

        # Filtrar previsões se necessário
        if product:
            val_df = val_df[val_df['name_product'] == product]
            test_df = test_df[test_df['name_product'] == product]
            future_df = future_df[future_df['name_product'] == product]

        if store:
            val_df = val_df[val_df['name_store'] == store]
            test_df = test_df[test_df['name_store'] == store]
            future_df = future_df[future_df['name_store'] == store]

        # Criar gráfico
        plt.figure(figsize=(15, 10))

        # Dados históricos
        plt.plot(historical_data['time_idx'], historical_data['order_qty'],
                 'b-', label='Dados Reais', linewidth=2)

        # Definir pontos de corte para visualização
        max_time_idx = self.df["time_idx"].max()
        test_cutoff = max_time_idx - self.max_prediction_length
        validation_cutoff = test_cutoff - self.max_prediction_length

        # Linhas verticais para separar períodos
        plt.axvline(x=validation_cutoff,
                    color='orange',
                    linestyle='--',
                    alpha=0.7,
                    label='Início Validação')
        plt.axvline(x=test_cutoff,
                    color='red',
                    linestyle='--',
                    alpha=0.7,
                    label='Início Teste')

        # Previsões de validação
        if not val_df.empty:
            val_aggregated = val_df.groupby('time_step')['prediction']\
                                   .sum().reset_index()
            val_time_idx = range(validation_cutoff + 1,
                                 validation_cutoff + len(val_aggregated) + 1)
            plt.plot(val_time_idx,
                     val_aggregated['prediction'],
                     'go-',
                     label='Previsões Validação',
                     markersize=6)

        # Previsões de teste
        if not test_df.empty:
            test_aggregated = test_df.groupby('time_step')['prediction']\
                                     .sum().reset_index()
            test_time_idx = range(test_cutoff + 1,
                                  test_cutoff + len(test_aggregated) + 1)
            plt.plot(test_time_idx,
                     test_aggregated['prediction'],
                     'ro-',
                     label='Previsões Teste',
                     markersize=6)

        # Previsões futuras
        if not future_df.empty:
            future_aggregated = future_df.groupby('time_step')['prediction']\
                                         .sum().reset_index()
            future_time_idx = range(max_time_idx + 1,
                                    max_time_idx + len(future_aggregated) + 1)
            plt.plot(future_time_idx,
                     future_aggregated['prediction'],
                     'mo-',
                     label='Previsões Futuras (3 meses)',
                     markersize=8, linewidth=2)

        plt.title(f'Previsões de Vendas: {product or "Todos Produtos"} - {store or "Todas Lojas"}')
        plt.xlabel('Time Index')
        plt.ylabel('Quantidade de Pedidos')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

        return val_df, test_df, future_df

    def get_future_predictions_summary(self):
        """Retorna resumo das previsões futuras por produto e loja"""
        future_df = self.get_prediction_dataframe(self.future_predictions,
                                                  "future")

        summary = future_df.groupby(['name_product', 'name_store']).agg({
            'prediction': ['sum', 'mean', 'std']
        }).round(2)

        summary.columns = ['Total_3_Meses', 'Media_Mensal', 'Desvio_Padrao']
        summary = summary.reset_index()

        return summary

    def run_complete_pipeline(self):
        """Executa o pipeline completo"""
        print("=== INICIANDO PIPELINE COMPLETO ===")

        (self.prepare_data()
             .create_datasets()
             .create_dataloaders()
             .create_model()
             .train_model()
             .make_predictions())

        print("=== PIPELINE CONCLUÍDO ===")
        return self
