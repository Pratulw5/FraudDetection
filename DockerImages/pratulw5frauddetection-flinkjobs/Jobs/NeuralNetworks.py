from river import linear_model, preprocessing, metrics

# Step 1: Define the model (Logistic Regression)
model = preprocessing.StandardScaler() | linear_model.LogisticRegression()

# Step 2: Define metric (Accuracy)
accuracy = metrics.Accuracy()

# Step 3: Simulate streaming data (one point at a time)

# Step 4: Process each data point as it arrives
for i, (x, y) in enumerate(data_stream):
    # Step 4a: Make a prediction
    y_pred = model.predict_one(x)
    print(f"Predicted: {y_pred}, Actual: {y}")
    
    # Step 4b: Update the model incrementally
    model = model.learn_one(x, y)
    
    # Step 4c: Update accuracy metric
    accuracy = accuracy.update(y, y_pred)
    print(f"Accuracy so far: {accuracy.get():.3f}\n")
