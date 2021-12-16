from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import math
import sys
import numpy as np
import tensorflow as tf

import dnn_feature_views as fv
# Step 4: Build and train a recall model.

uv_size = 128  # Dimension of the user/item vector.

graph = tf.Graph()

with graph.as_default():
    # hidden layer
    W1 = tf.Variable(tf.truncated_normal([fv.embedding_size, uv_size]))
    b1 = tf.Variable(tf.truncated_normal([uv_size]))
    #W2 = tf.Variable(tf.truncated_normal([256, uv_size]))
    #b = tf.Variable(tf.truncated_normal([uv_size]))

    # Input data.
    train_dataset = tf.sparse_placeholder(tf.int32, shape=[fv.batch_size, fv.vocabulary_size])
    train_labels = tf.placeholder(tf.int32, shape=[fv.batch_size, 1])

    # Ops and variables pinned to the CPU because of missing GPU implementation
    with tf.device('/cpu:0'):
        # Look up embeddings for inputs.
        embeddings = tf.Variable(
            tf.random_uniform([fv.vocabulary_size, fv.embedding_size], -1.0, 1.0))

        # Construct the variables for the NCE loss
        nce_weights = tf.Variable(
            tf.truncated_normal([fv.vocabulary_size, uv_size],
                                stddev=1.0 / math.sqrt(uv_size)))
        nce_biases = tf.Variable(tf.zeros([fv.vocabulary_size]))

        embed = tf.nn.embedding_lookup_sparse(embeddings,train_dataset, sp_weights=None, combiner="mean")

        layer1 = tf.nn.relu(tf.matmul(embed, W1) + b1)
        #layer2 = tf.nn.relu(tf.matmul(layer1, W2))#) + b)
        #Compute the average NCE loss for the batch.
        #tf.nce_loss automatically draws a new sample of the negative labels each
        #time we evaluate the loss.
    loss = tf.reduce_mean(
        tf.nn.nce_loss(weights=nce_weights,
                       biases=nce_biases,
                       labels=train_labels,
                       inputs=layer1,
                       num_sampled=fv.num_sampled,
                       partition_strategy="div",
                       num_classes=fv.vocabulary_size))

    # Construct the SGD optimizer using a learning rate of 1.0.
    optimizer = tf.train.AdamOptimizer(0.01).minimize(loss)

    # Add variable initializer.
    init = tf.global_variables_initializer()
    saver = tf.train.Saver()
# Step 5: Begin training.

with tf.Session(graph=graph) as session:
    # We must initialize all variables before we use them.
    init.run()
    print("Initialized")
    checkpoint_dir = "./ckpt/"
    infer_ckpt = tf.train.get_checkpoint_state(checkpoint_dir)
    if infer_ckpt and infer_ckpt.model_checkpoint_path:
        print("Use the model {}".format(infer_ckpt.model_checkpoint_path))
        saver.restore(session, infer_ckpt.model_checkpoint_path)
    for step in range(fv.num_steps):
        step += 1
        batch_inputs, batch_labels = fv.generate_batch(fv.batch_size)
        feed_dict = {train_dataset: batch_inputs, train_labels: batch_labels}

        # We perform one update step by evaluating the optimizer op (including it
        # in the list of returned values for session.run()
        _, loss_val = session.run([optimizer, loss], feed_dict=feed_dict)

        if step % 100 == 0:
            print("Average loss at step ", step, ": ", loss_val)
            average_loss = 0
        if step % 100000 == 0:
            save_path = saver.save(session, checkpoint_dir+'model.ckpt',global_step=step)
            print("Model saved in file: %s" % save_path)

    np.savetxt(fv.itemvec, nce_weights.eval())
