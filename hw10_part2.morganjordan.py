###
###
# Author Info:
#     This code is modified from code originally written by Jim Blomo and Derek Kuo


##/flags account suspected of belonging to a user who already has another account; based on the information that these two accounts have always rated the same restaurants  


from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep
from math import *




class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_user_business(self,_,record):
        """Taken a record, yield <user_id, business_id>"""
        # print([record['user_id'], record['business_id']])
        yield [record['user_id'], record['business_id']] #yields a list of [user acount, business id reviewed]

    def reducer1_compile_businesses_under_user(self,user_id,business_ids):
        # print(user_id, list(business_ids))
        business_ids_dedup = []
        ###
        # TODO_1: compile businesses as a list of array under given user_id,after remove duplicate business, yield <user_id, [business_ids]>
        # # print(user_id, business_ids_dedup)
        business_ids = set(business_ids)
        yield user_id, list(business_ids)
        

    def mapper2_collect_businesses_under_user(self, user_id, business_ids):
        
        ###
        # TODO_2: collect all <user_id, business_ids> pair, map into the same Keyword LIST, yield <'LIST',[user_id, [business_ids]]>
        yield ['LIST',[user_id, business_ids]]

    def reducer2_calculate_similarity(self,stat,user_business_ids):
        def Jaccard_similarity(business_list1, business_list2):
            
            ###
            # TODO_3: Implement Jaccard Similarity here, output score should between 0 to 1
            ##
            # intersection_cardinality = len(set.intersection(*set(business_list1), set(business_list2))) # http://dataconomy.com/implementing-the-five-most-popular-similarity-measures-in-python/
            # union_cardinality = len(set.union(*set(business_list1), set(business_list2)))
            # jaccard_sim = intersection_cardinality/float(union_cardinality)
            # return jaccard_sim

            business_list1 = set(business_list1)
            business_list2 = set(business_list2)
            intersect_biz = list(set(business_list1.intersection(business_list2)))
            union_biz = list(set(business_list1.union(business_list2)))
            # print(len(intersect_biz), "/", len(union_biz))
            jac_sim = float(len(intersect_biz)/len(union_biz)) 
            return jac_sim
    
        ###
        # TODO_4: Calulate Jaccard, output the pair users that have similarity over 0.5, yield <[user1,user2], similarity>
        ##/
        
   
        user_business_ids = list(user_business_ids)
        # print(user_business_ids)
        for x in range(0, len(user_business_ids)-1):
            # print(x)
            for i in range(x+1, len(user_business_ids)):
                # print("second for", i)
                # >> similarity = Jaccard_similarity(bis-list[x][1]), bis-list[i][1])
                # if sim>= 0.5:
                # yield[[biz-id-list[x][0], biz-id-list[i][0], jaccard]
                user1 = user_business_ids[x][0]
                # print(user1)
                business_list1 = user_business_ids[x][1]
                # print(business_list1)
                user2 = user_business_ids[i][0]
                business_list2 = user_business_ids[i][1]
                # print(business_list2)
                
                jaccard = Jaccard_similarity(business_list1, business_list2)
        # print("you got this far!")
                if jaccard >= 0.5:
                    yield [user1, user2], jaccard
                

    def steps(self):
        return [
            MRStep(mapper=self.mapper1_extract_user_business, reducer=self.reducer1_compile_businesses_under_user),
            MRStep(mapper=self.mapper2_collect_businesses_under_user, reducer= self.reducer2_calculate_similarity)
        ]


if __name__ == '__main__':
    UserSimilarity.run()
