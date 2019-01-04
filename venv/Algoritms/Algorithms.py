#http://www.speech.zone/exercises/dtw-in-python/the-final-dtw-code/
#pip install fastdtw
import math

class Algorithms:

#### interval, multivariate DTW

    def local_distance_idtw(training_frame, i, test_frame, j):
        """
        Compute the Euclidean distance between two feature 'vectors' (which must actually be ints)
        """
        distance = 0

        for k in range(len(training_frame)):
            train_temp = training_frame[k][i]
            test_temp = test_frame[k][j]
            if (train_temp < 0 or test_temp < 0):
                continue
            else:
                distance += math.pow(train_temp - test_temp, 2)
        return math.sqrt(distance)



    def iDTW(training_patient, testing_patient):
        global_distance = 0
        alignment = []
        i = 0;
        j = 0 ;

        backpointer = []
        empty_backpointer = (None, None)
        for i in range(len(training_patient[0])):
            this_row = []
        for j in range(len(testing_patient[0])):
            this_row.append(empty_backpointer)
        backpointer.append(this_row)

        global_distance = []
        dummy_value = -1
        for i in range(len(training_patient[0])):
            this_row = []
            for j in range(len(testing_patient[0])):
                this_row.append(dummy_value)
            global_distance.append(this_row)

        for i in range (len(training_patient[0])):
            for j in range(len(testing_patient[0])):

                #global_distance[0][0]
                if (i == 0) and (j == 0):
                    global_distance[i][j] = Algorithms.local_distance_idtw(training_patient, i, testing_patient, j)

                # global_distance[0][X]
                elif (i == 0):
                    assert global_distance[i][j - 1] >= 0
                    global_distance[i][j] = global_distance[i][j - 1] + Algorithms.local_distance_idtw(training_patient, i, testing_patient, j)
                    #backpointer[i][j] = (i, j - 1)

                # global_distance[X][0]
                elif (j == 0):
                    assert global_distance[i - 1][j] >= 0
                    global_distance[i][j] = global_distance[i - 1][j] + Algorithms.local_distance_idtw(training_patient, i, testing_patient, j)
                    #backpointer[i][j] = (i - 1, j)

                else :
                    assert global_distance[i][j - 1] >= 0
                    assert global_distance[i - 1][j] >= 0
                    assert global_distance[i - 1][j - 1] >= 0

                    #check the min distance neighbor
                    lowest_global_distance = global_distance[i - 1][j]

                    if global_distance[i][j - 1] < lowest_global_distance:
                        lowest_global_distance = global_distance[i][j - 1]

                    if global_distance[i - 1][j - 1] < lowest_global_distance:
                        lowest_global_distance = global_distance[i - 1][j - 1]

                    global_distance[i][j] = lowest_global_distance + Algorithms.local_distance_idtw(training_patient, i, testing_patient, j)

            # the best possible global distance is just the value in the "last" corner of the matrix
            #  (remembering that everything is indexed from 0)

        return global_distance[len(training_patient[0]) - 1][len(testing_patient[0]) - 1]




    ################################ simularity function for Basic DTW ##############################


    def basic_local_distance(template_frame, test_frame):
        """
        Compute the Euclidean distance between two feature 'vectors' (which must actually be ints)
        """
        assert type(template_frame) == type(test_frame) == int

        return math.sqrt(pow(template_frame - test_frame, 2))
















