from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Assuming you have the labeled DataFrame named 'labeled_df'
# with columns ['filed_date', 'feature_text', 'label_2_weeks', 'label_12_weeks']

# Separate the features and labels
X = labeled_df['feature_text']
y_2_weeks = labeled_df['label_2_weeks']
y_12_weeks = labeled_df['label_12_weeks']

# Convert text features to numerical representation using TF-IDF
vectorizer = TfidfVectorizer()
X_vectorized = vectorizer.fit_transform(X)

# Split the data into training and testing sets
X_train, X_test, y_train_2_weeks, y_test_2_weeks = train_test_split(X_vectorized, y_2_weeks, test_size=0.2, random_state=42)
X_train, X_test, y_train_12_weeks, y_test_12_weeks = train_test_split(X_vectorized, y_12_weeks, test_size=0.2, random_state=42)

# Create and train the random forest classifier for 2 weeks prediction
rf_classifier_2_weeks = RandomForestClassifier(n_estimators=100, random_state=42)
rf_classifier_2_weeks.fit(X_train, y_train_2_weeks)

# Create and train the random forest classifier for 12 weeks prediction
rf_classifier_12_weeks = RandomForestClassifier(n_estimators=100, random_state=42)
rf_classifier_12_weeks.fit(X_train, y_train_12_weeks)

# Make predictions on the test set for 2 weeks
y_pred_2_weeks = rf_classifier_2_weeks.predict(X_test)

# Make predictions on the test set for 12 weeks
y_pred_12_weeks = rf_classifier_12_weeks.predict(X_test)

# Calculate evaluation metrics for 2 weeks prediction
accuracy_2_weeks = accuracy_score(y_test_2_weeks, y_pred_2_weeks)
precision_2_weeks = precision_score(y_test_2_weeks, y_pred_2_weeks)
recall_2_weeks = recall_score(y_test_2_weeks, y_pred_2_weeks)
f1_2_weeks = f1_score(y_test_2_weeks, y_pred_2_weeks)

# Calculate evaluation metrics for 12 weeks prediction
accuracy_12_weeks = accuracy_score(y_test_12_weeks, y_pred_12_weeks)
precision_12_weeks = precision_score(y_test_12_weeks, y_pred_12_weeks)
recall_12_weeks = recall_score(y_test_12_weeks, y_pred_12_weeks)
f1_12_weeks = f1_score(y_test_12_weeks, y_pred_12_weeks)

# Print the evaluation metrics
print("2 Weeks Prediction:")
print("Accuracy:", accuracy_2_weeks)
print("Precision:", precision_2_weeks)
print("Recall:", recall_2_weeks)
print("F1 Score:", f1_2_weeks)

print("\n12 Weeks Prediction:")
print("Accuracy:", accuracy_12_weeks)
print("Precision:", precision_12_weeks)
print("Recall:", recall_12_weeks)
print("F1 Score:", f1_12_weeks)