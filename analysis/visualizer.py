from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy.interpolate import interp1d
from scipy.signal import savgol_filter
from db.query_manager import QueryManager

class Visualizer:

    colors       = ['#344e79', '#51a063', '#c44e52', '#8172b2', '#ccb974', '#64b5cd']
    colors_light = ['#547fc4', '#78ed93', '#ff656b', '#b9a3ff', '#ffe791', '#7ce1ff']

    def __init__(self, config_path = None, train_set_size = 0.7):
        if config_path is not None:
            self.qm = QueryManager(config_path, train_set_size)

        self.log = None

    def show_order_time_distribution(self, data_type = 'all', time_units_num = 20):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'
        time_units_num
            how many time units should chart have, default = 20
        """

        data = self.qm.query_order_time_distribution(data_type)
        plt  = self.get_time_plot(data, time_units_num, 'order', 'Broj narudzbi')

        plt.show()

    def show_user_time_distribution(self, data_type = 'all', time_units_num = 20):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'
        time_units_num
            how many time units should chart have, default = 20
        """

        data = self.qm.query_user_time_distribution(data_type)
        plt  = self.get_time_plot(data, time_units_num, 'user', 'Broj korisnika')

        plt.show()


    def show_first_purchase_time_distribution(self, data_type = 'all', time_units_num = 20):
        purchases  = self.qm.query_first_purchase_dates(data_type)
        timestamps = self.qm.query_timestamps(data_type)
        data       = []
        for timestamp in timestamps:
            if filter(lambda purchase: purchase['first_purchase'] == timestamp['timestamp'], purchases):
                timestamp['purchase'] = 1
            else:
                timestamp['purchase'] = None
            data.append(timestamp)

        plt = self.get_time_plot(data, time_units_num, 'purchase', 'Broj prvih kupnji')
        plt.show()

    def show_items_per_order_distrubtion(self, data_type = 'all'):
        purchases_per_orders = self.qm.query_items_per_order(data_type)
        orders_total         = float(self.qm.query_orders_total(data_type)[0]['orders_total'])

        x = []
        y = []

        for ppo in purchases_per_orders:
            x.append(ppo['items_num'])
            y.append(ppo['orders_num'] / orders_total * 100)

        x_ticks = np.arange(1, x[len(x)-1]+1)
        plt.xticks(x_ticks)

        self._set_style('Broj predmeta u narudzbi', 'Udio narudzbi (%)')
        plt.bar(x, y)

        plt.show()

    def show_items_per_part_of_day(self, data_type = 'all'):
        parts_of_day = ['Morning', 'Afternoon', 'Evening', 'Night']
        x_values     = ['Jutro', 'Poslijepodne', 'Vecer', 'Noc']

        self._show_items_per_tf_property(
            parts_of_day, x_values,
            self.qm.query_items_total_for_part_of_day,
            'Dio dana', 'Udio narucenih predmeta (%)', data_type
        )

    def show_items_per_day_in_week(self, data_type = 'all'):
        day_in_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        x_values    = ['Pon', 'Uto', 'Sri', 'Cet', 'Pet', 'Sub', 'Ned']

        self._show_items_per_tf_property(
            day_in_week, x_values,
            self.qm.query_items_total_for_day_in_weel,
            'Dan u tjednu', 'Udio narucenih predmeta (%)', data_type
        )


    def show_precision_recall_plot(self, k, precisions, recalls, labels):
        if len(precisions) != len(recalls) != len(labels):
            return

        x_trans = np.arange(len(k))
        plt.xticks(x_trans, k)

        for i in range(0, len(labels)):
            plt.plot(x_trans, precisions[i], color=self.colors[i], marker='o', markersize=4)
            plt.plot(x_trans, recalls[i], color=self.colors[i], linestyle='dashed', marker='o', markersize=4)

        plt.yticks(np.arange(0, 1.1, 0.1))
        self._set_style('k', 'P/R')

        labels_all = []
        for label in labels:
            labels_all.append(label + '(P)')
            labels_all.append(label + '(R)')
        plt.legend(labels_all)
        plt.show()

    def show_f1_plot(self, k, f1, labels):
        if len(f1) != len(labels):
            return

        x_trans = np.arange(len(k))
        plt.xticks(x_trans, k)

        for i in range(0, len(labels)):
            plt.plot(x_trans, f1[i], color=self.colors[i], marker='o', markersize=4)

        plt.yticks(np.arange(0, 1.1, 0.1))
        self._set_style('k', 'F1')

        plt.legend(labels)
        plt.show()

        pass

    def show_roc_plot(self, fallout, recall, label, show_auc = True):
        color_index = 2
        fallout = [0] + fallout
        recall  = [0] + recall
        plt.plot(fallout, recall, color=self.colors[color_index])
        if fallout[-1] < recall[-1]:
            roc_random = [0, fallout[-1]]
        else:
            roc_random = [0, recall[-1]]

        plt.plot(roc_random, roc_random, linestyle='dotted', color='#777777')

        if show_auc:
            plt.fill(fallout + fallout[-1:], recall + [0], color=self.colors_light[color_index])

        plt.yticks(np.arange(0, 1.1, 0.1))

        self._set_style('F', 'R')
        plt.legend([label])
        plt.show()

    def show_algorithm_time_bar_chart(self, x_labels, bar_labels, time_data):
        ind = np.arange(len(x_labels)) # the x locations for the groups
        # for data_dim in time_data:
        #     plt.bar(range(len(data_dim)), data_dim)

        for i in range(0, len(time_data)):
            if i == 0:
                plt.bar(ind, time_data[0])
            else:
                bottom = None
                for j in range(0, i):
                    if bottom is None:
                        bottom = np.array(time_data[j])
                    else:
                        bottom += np.array(time_data[j])
                plt.bar(ind, time_data[i], bottom = bottom )

        plt.ylabel('Vrijeme(s)')
        plt.title('')
        plt.xticks(ind, x_labels)
        # plt.yticks(np.arange(0, 81, 10))
        # plt.legend((p1[0], p2[0]), ('Men', 'Women'))
        plt.legend(bar_labels)

        plt.show()
        pass

    def _show_items_per_tf_property(self, property_values, x_values, query_function, x_label, y_label, data_type = 'all' ):
        x         = np.arange(0, len(x_values))
        items_num = []
        total     = 0.0

        for property_value in property_values:
            it = query_function(property_value, data_type)
            items_num.append(it[0]['items_total'])
            total += it[0]['items_total']

        for i in range(0, len(items_num)):
            items_num[i] /= total
            items_num[i] *= 100

        plt.xticks(x, x_values)

        self._set_style(x_label, y_label)
        plt.bar(x, items_num)

        plt.show()

    def get_time_plot(self, data, time_units_num, data_dim_name, y_label):
        x = np.arange(time_units_num)
        y = self._crunch_data(data, time_units_num, data_dim_name)

        start_timestamp = data[0]['timestamp']
        end_timestamp   = data[-1]['timestamp']

        # smooth the data
        x_s = np.linspace(x.min(), x.max(), time_units_num * 100)
        itp = interp1d(x, y, kind='linear')
        y_s = savgol_filter(itp(x_s), 101, 3)

        start = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
        end   = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')

        self._set_style('Vrijeme (' + start.strftime('%d.%m.%y.') + ' - ' + end.strftime('%d.%m.%y.') + ')', y_label)
        plt.plot(x_s, y_s)

        return plt

    def _set_style(self, x_label, y_label, title = ''):
        sns.set_style("whitegrid")

        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)

        plt.tight_layout()
        plt.grid(alpha=0.8)

    def _crunch_data(self, data, size, data_dim_name):
        counter       = 0
        step          = len(data) / float(size)
        step_break    = step
        step_value    = 0
        crunched_data = []

        for data_unit in data:
            counter += 1
            if counter > step_break:
                step_break += step
                crunched_data.append(step_value)
                step_value = 0
            elif data_unit[data_dim_name] is not None:
                step_value += 1
        crunched_data.append(step_value)

        return np.array(crunched_data)
