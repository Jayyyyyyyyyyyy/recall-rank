from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys
import collections
import operator
import numpy as np
import tensorflow as tf

infer=False
num_steps=15000
vocabulary_size = 20000
inputdata = sys.argv[1]
itemvec = sys.argv[2]
vocabfile = sys.argv[3]
batch_size = 1024
embedding_size = 128  # Dimension of the embedding vector.
uv_size = 128  # Dimension of the user/item vector.

# We pick a random validation set to sample nearest neighbors. Here we limit the
# validation samples to the items that have a low numeric ID, which by
# construction are also the most frequent.
num_sampled = 32  # Number of negative examples to sample.


def save_vocab(vocab):
    with open(vocabfile, "w") as f:
        for k, v in vocab.items():
            f.write(str(k) + "\t" + str(v) + "\n")


# Read the data into a list of strings.
def read_data(inputdata):
    with open(inputdata) as f:
        # data = tf.compat.as_str(f.read()).split()
        data = f.readlines()
    return data


data = read_data(inputdata)
print('Data size', len(data))

# Step 2: Build the dictionary and replace rare items with UNK token.


def build_dataset(data, vocabulary_size):
    word_count = collections.defaultdict(int)
    for line in data:
        words = line.strip().split(" ")
        for w in words:
            word_count[w] += 1
    sorted_word_count = dict(sorted(word_count.items(), key=operator.itemgetter(1), reverse=True)[:vocabulary_size])
    #print(sorted_word_count)
    dictionary = dict(zip(sorted_word_count.keys(), range(0, vocabulary_size)))
    reverse_dictionary = dict(zip(range(0, vocabulary_size), sorted_word_count.keys()))
    save_vocab(reverse_dictionary)
    return data, word_count, dictionary, reverse_dictionary


data, word_count, dictionary, reverse_dictionary = build_dataset(data, vocabulary_size)
print('dict size', len(dictionary))
data_index = 0
buffer = []


# Step 3: Function to generate a training batch for the cbow model.
def generate_batch(batch_size):
    global data_index
    global buffer
    # labels = np.ndarray(shape=(batch_size, 1), dtype=np.int32)

    indices = []
    values = []
    labels = np.ndarray(shape=(batch_size, 1), dtype=np.int32)
    while len(buffer) < batch_size:
        words = data[data_index].strip().split(" ")
        ids = []
        for i in range(len(words)):
            if words[i] in dictionary:
                ids.append(dictionary[words[i]])
        if len(ids) > 1:
            for i in range(len(ids)):
                buffer.append((ids[i], ids[:i] + ids[i + 1:]))
        data_index = (data_index + 1) % len(data)

    for i in range(batch_size):
        label, instance = buffer.pop(0)
        labels[i][0] = label
        for v in instance:
            indices.append([i, v])
            values.append(v)

    batch = tf.SparseTensorValue(indices=indices, values=values, dense_shape=[batch_size, vocabulary_size])
    return batch, labels
