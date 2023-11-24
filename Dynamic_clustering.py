from scipy.spatial import distance
import numpy as np
# Assign all the points to the nearest centroid based on distance matrix
# The maximum priority should be given to heighest weight -> distance calculation = actual distance / weight
# each cluster can have a upper limit of weight
def assigning_cluster_id(upper_limit, dist_mat_df, final_df, initial_centers, year='2019', process_flag = 'depot'):
    index_number_initial = final_df.index.tolist()
    if process_flag == 'depot':
        other_centers = [x for x in index_number_initial if x not in initial_centers]
        index_number = initial_centers + other_centers
    else :
        index_number = index_number_initial
    cluster_dist_dict = dict()
    cluster_ids = []
    total_dist = 0
    for pt_idx in index_number:
        dist = [dist_mat_df.at[pt_idx, str(center)] for center in initial_centers]
        #print(f'The minimum distance is {min(dist)}')
        sorted_dist = sorted(dist)
        min_max_index = list(map(lambda x : dist.index(x), sorted_dist))
#         breakpoint()
        cluster_flag = 0
        if final_df.at[pt_idx,year] > 100 :
            max_opportunity = 5
        elif process_flag == 'refinery':
            max_opportunity = 3
        else:
            max_opportunity = 3
        for min_idx in min_max_index[:max_opportunity]:
            if min_idx not in cluster_dist_dict.keys():
                cluster_dist_dict[min_idx] = final_df.at[pt_idx,year]
                cluster_flag = 1
                break
            elif cluster_dist_dict[min_idx] + final_df.at[pt_idx,year] <= upper_limit:
                cluster_dist_dict[min_idx] += final_df.at[pt_idx,year]
                cluster_flag = 1
                break
            else:
                continue
        if cluster_flag == 1:
            cluster_ids.append(min_idx)
        elif cluster_flag == 0:
            cluster_ids.append(np.nan)
        above_threshold = any(value > upper_limit for value in cluster_dist_dict.values())
        if above_threshold:
            print('The clusters are diverging')
            break
        #print(f'The acceptable min distance is :{dist[min_idx]}')
        total_dist += dist[min_idx]
    # update the centroid
    if process_flag == 'depot':
        final_df = final_df.reindex(index=index_number)
    final_df['cluster_id'] = cluster_ids
#     display(final_df_2010)
    return final_df, cluster_dist_dict, total_dist

def calculate_new_cluster_centers(final_df, probable_center_df, max_cluster, year='2019'):
    new_cluster_centers = []
    for prob_idx in probable_center_df.index:
        dist_list = [[real_idx, (distance.euclidean((final_df.at[real_idx,'Latitude'],\
                                                    final_df.at[real_idx,'Longitude']),\
                                                   (probable_center_df.at[prob_idx,'Latitude'],\
                                                    probable_center_df.at[prob_idx,'Longitude'])))/ final_df.at[real_idx, year]]\
                    for real_idx in final_df.index]
        sorted_dist_lst = sorted(dist_list, key = lambda sublist : sublist[1])
        for sublist in sorted_dist_lst:
            if sublist[0] not in new_cluster_centers:
                new_cluster_centers.append(sublist[0])
                break
            else:
                continue
#         min_dist_lst = min(dist_list, key = lambda sublist : sublist[1])
#         sorted_dist_lst = sorted(dist_list, key = lambda sublist : sublist[1])
#         min_dist_lst = sorted_dist_lst[:max_cluster]
#         #breakpoint()
#         prod_min_dist_lst = [[sublist[0], final_df.at[sublist[0], year]] for sublist in min_dist_lst]
#         max_prod_lst = max(prod_min_dist_lst, key = lambda sublist : sublist[1])
#         print(f'min_dist_lst {min_dist_lst}')
#         print(f'prod_min_dist_lst {prod_min_dist_lst}')
#         print(f'max_prod {max_prod_lst[0]}')
#         new_cluster_centers.append(min_dist_lst[0])
    return new_cluster_centers