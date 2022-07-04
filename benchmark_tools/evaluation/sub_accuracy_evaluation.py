import json
from benchmark_tools.evaluation.base import BaseEvaluation


class COCOSubscriptionAccuracyEvaluation(BaseEvaluation):

    def __init__(self, *args, **kwargs):
        super(COCOSubscriptionAccuracyEvaluation, self).__init__(*args, **kwargs)
        self.subscription_jl = kwargs['subscription_jl']
        self.dataset_annotations_json = kwargs['dataset_annotations_json']
        self.dataset_frameindex_json = kwargs['dataset_frameindex_json']
        self.class_label = kwargs['class_label']

        self.annotations = self.get_annotations_data(self.dataset_annotations_json)
        self.category_id_name_map = self.get_category_id_name_map(self.annotations['categories'])
        self.frame_index_mapping = self.get_frame_index_to_image_id(self.dataset_frameindex_json)
        self.image_name_to_id = self.get_image_name_to_id_mapping(self.annotations)
        self.image_ids_to_categories = self.get_image_ids_to_categories_mapping(self.annotations)
        self.image_id_to_events = self.get_image_id_to_events_mapping(self.subscription_jl)

    def get_image_id_to_events_mapping(self, subscription_jl):
        data = {}
        with open(subscription_jl, 'r') as jl_file:
            for json_line in jl_file.readlines():
                vekg_stream = json.loads(json_line).get('vekg_stream', [])
                for event in vekg_stream:
                    frame_index = event['frame_index']
                    image_name = self.frame_index_mapping[frame_index]
                    image_id = self.image_name_to_id[image_name]
                    data[image_id] = event
        return data

    def get_image_ids_to_categories_mapping(self, annotations):
        image_ids_map = {}

        for annotation in annotations['annotations']:
            image_id = annotation['image_id']
            image_categories = image_ids_map.setdefault(image_id, set())
            category_name = self.category_id_name_map[annotation['category_id']]
            image_categories.add(category_name)
            image_ids_map[image_id] = image_categories
        return image_ids_map

    def get_image_name_to_id_mapping(self, annotations):
        return {
            img_ann['file_name']: img_ann['id']
            for img_ann in annotations['images']
        }

    def get_frame_index_to_image_id(self, dataset_frameindex_json):
        with open(dataset_frameindex_json, 'r') as f:
            frame_index_str_to_image_id = json.load(f)
            frame_index_to_image_id = {
                int(k): v
                for k, v in frame_index_str_to_image_id.items()
            }
        return frame_index_to_image_id

    def get_category_id_name_map(self, category_annotations):
        category_id_to_name = {}
        for category in category_annotations:
            category_id_to_name[category['id']] = category['name']
        return category_id_to_name

    def get_annotations_data(self, dataset_annotations_json):
        with open(dataset_annotations_json, 'r') as annotation_file:
            annotations = json.load(annotation_file)
        return annotations

    def get_image_categories(self, image_annotations, category_id_name_map, image_id):
        annotations_data = []
        for annotation in image_annotations:
            if annotation['image_id'] == image_id:
                category_name = category_id_name_map[annotation['category_id']]
                annotations_data.append(category_name)
        annotations_data = list(set(annotations_data))
        return annotations_data

    def calculate_accuracy(self):
        true_positives = len(self.true_positives_events)
        true_negatives = len(self.true_negatives_events)
        false_positives = len(self.false_positives_events)
        false_negatives = len(self.false_negatives_events)

        nominator = true_positives + true_negatives
        denominator = true_positives + true_negatives + false_positives + false_negatives
        accuracy = nominator / denominator
        return accuracy

    def calculate_precision(self):
        true_positives = len(self.true_positives_events)
        false_positives = len(self.false_positives_events)

        return true_positives / (true_positives + false_positives)

    def calculate_recall(self):
        true_positives = len(self.true_positives_events)
        false_negatives = len(self.false_negatives_events)
        return true_positives / (true_positives + false_negatives)

    def calculate_f_score(self, precision, recall):
        nominator = 2 * (precision * recall)
        denominator = precision + recall
        f_1 = nominator / denominator
        return f_1

    def calculate_metrics(self):
        self.accuracy = self.calculate_accuracy()
        self.precision = self.calculate_precision()
        self.recall = self.calculate_recall()
        self.f_score = self.calculate_f_score(self.precision, self.recall)
        results = {
            'accuracy': self.accuracy,
            'precision': self.precision,
            'recall': self.recall,
            'f_score': self.f_score,
        }
        return results

    def prepare_false_and_true_positives_and_negatives(self):
        self.true_positives_events = {}
        self.false_positives_events = {}
        self.true_negatives_events = {}
        self.false_negatives_events = {}

        for image_id, categories in self.image_ids_to_categories.items():
            is_true = self.class_label in categories
            is_positive = image_id in self.image_id_to_events.keys()
            if is_positive:
                if is_true:
                    self.true_positives_events[image_id] = self.image_id_to_events[image_id]
                else:
                    self.false_positives_events[image_id] = self.image_id_to_events[image_id]

            # is negative
            else:
                # true negative
                if not is_true:
                    self.true_negatives_events[image_id] = None
                else:
                    self.false_negatives_events[image_id] = None

    def run(self):
        self.logger.debug(f'Evaluation for Subscription accuracy for {self.class_label} class is running for {len(self.image_id_to_events)}')
        self.prepare_false_and_true_positives_and_negatives()
        results = self.calculate_metrics()
        return self.verify_thresholds(results)


def run(subscription_jl, dataset_annotations_json, dataset_frameindex_json, class_label, threshold_functions, logging_level):
    evaluation = COCOSubscriptionAccuracyEvaluation(
        subscription_jl=subscription_jl,
        dataset_annotations_json=dataset_annotations_json,
        dataset_frameindex_json=dataset_frameindex_json,
        class_label=class_label,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "subscription_jl": "./outputs/subscription_3-1.jl",
        "dataset_annotations_json": "./datasets/coco2017/annotations/instances_val2017.json",
        "dataset_frameindex_json": "datasets/coco2017/frame_indexes/coco2017-val-300x300-30fps.json",
        "class_label": "person",
        "threshold_functions": {
            "accuracy": "lambda x: x > 0.5",
            "precision": "lambda x: x > 0.5",
            "recall": "lambda x: x > 0.9",
            "f_score": "lambda x: x > 0.6",
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
