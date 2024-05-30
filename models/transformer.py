import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import BertTokenizer, BertForSequenceClassification, AdamW
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.model_selection import train_test_split


df = pd.read_csv('./saved_train_data.csv')

# Prepare the data
texts = df['features'].tolist()
labels_2_weeks = df['label_2_weeks'].tolist()
labels_12_weeks = df['label_12_weeks'].tolist()

# Split the data into train and test sets
train_texts, test_texts, train_labels_2_weeks, test_labels_2_weeks, train_labels_12_weeks, test_labels_12_weeks = train_test_split(
    texts, labels_2_weeks, labels_12_weeks, test_size=0.2, random_state=42)

# Load the pre-trained BERT model and tokenizer
model_name = 'bert-base-uncased'
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertForSequenceClassification.from_pretrained(model_name, num_labels=2)  # Assuming binary classification

# Tokenize the text data
train_encodings = tokenizer(train_texts, truncation=True, padding=True)
test_encodings = tokenizer(test_texts, truncation=True, padding=True)

# Create PyTorch datasets
class TextDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

train_dataset_2_weeks = TextDataset(train_encodings, train_labels_2_weeks)
test_dataset_2_weeks = TextDataset(test_encodings, test_labels_2_weeks)
train_dataset_12_weeks = TextDataset(train_encodings, train_labels_12_weeks)
test_dataset_12_weeks = TextDataset(test_encodings, test_labels_12_weeks)

# Create data loaders
train_loader_2_weeks = DataLoader(train_dataset_2_weeks, batch_size=2, shuffle=True)
test_loader_2_weeks = DataLoader(test_dataset_2_weeks, batch_size=2)
train_loader_12_weeks = DataLoader(train_dataset_12_weeks, batch_size=2, shuffle=True)
test_loader_12_weeks = DataLoader(test_dataset_12_weeks, batch_size=2)

# Move the model to the GPU
device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
model.to(device)

# Fine-tune the model for label_2_weeks
optimizer = AdamW(model.parameters(), lr=2e-5)
num_epochs = 8
for epoch in range(num_epochs):
    model.train()
    for batch in train_loader_2_weeks:
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        labels = batch['labels'].to(device)
        outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
        loss = outputs.loss
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

# Evaluate the model for label_2_weeks
model.eval()
predictions_2_weeks = []
true_labels_2_weeks = []
with torch.no_grad():
    for batch in test_loader_2_weeks:
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        outputs = model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        predictions_2_weeks.extend(torch.argmax(logits, dim=1).tolist())
        true_labels_2_weeks.extend(batch['labels'].tolist())

accuracy_2_weeks = accuracy_score(true_labels_2_weeks, predictions_2_weeks)
precision_2_weeks = precision_score(true_labels_2_weeks, predictions_2_weeks)
recall_2_weeks = recall_score(true_labels_2_weeks, predictions_2_weeks)
f1_2_weeks = f1_score(true_labels_2_weeks, predictions_2_weeks)

print("Results for label_2_weeks:")
print(f"Accuracy: {accuracy_2_weeks:.4f}")
print(f"Precision: {precision_2_weeks:.4f}")
print(f"Recall: {recall_2_weeks:.4f}")
print(f"F1-score: {f1_2_weeks:.4f}")

# Fine-tune the model for label_12_weeks
optimizer = AdamW(model.parameters(), lr=2e-5)
num_epochs = 8
for epoch in range(num_epochs):
    model.train()
    for batch in train_loader_12_weeks:
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        labels = batch['labels'].to(device)
        outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
        loss = outputs.loss
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

# Evaluate the model for label_12_weeks
model.eval()
predictions_12_weeks = []
true_labels_12_weeks = []
with torch.no_grad():
    for batch in test_loader_12_weeks:
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        outputs = model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        predictions_12_weeks.extend(torch.argmax(logits, dim=1).tolist())
        true_labels_12_weeks.extend(batch['labels'].tolist())

accuracy_12_weeks = accuracy_score(true_labels_12_weeks, predictions_12_weeks)
precision_12_weeks = precision_score(true_labels_12_weeks, predictions_12_weeks)
recall_12_weeks = recall_score(true_labels_12_weeks, predictions_12_weeks)
f1_12_weeks = f1_score(true_labels_12_weeks, predictions_12_weeks)

print("Results for label_12_weeks:")
print(f"Accuracy: {accuracy_12_weeks:.4f}")
print(f"Precision: {precision_12_weeks:.4f}")
print(f"Recall: {recall_12_weeks:.4f}")
print(f"F1-score: {f1_12_weeks:.4f}")