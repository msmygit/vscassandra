#include <unistd.h>
#include "org_apache_cassandra_index_sai_disk_pgm_PGMIndex.h"
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "pgm/pgm_index.hpp"

#include <boost/static_assert.hpp>
BOOST_STATIC_ASSERT(sizeof(jlong)>=sizeof(void *));

const int epsilon = 256;

JNIEXPORT void JNICALL Java_org_apache_cassandra_index_sai_disk_pgm_PGMIndex_create
(JNIEnv *env, jobject what, jlongArray array, jobject segmentList) {
    std::vector<long> data;
    jlong *elements = env->GetLongArrayElements(array, 0);
    jsize len = env->GetArrayLength(array);

    for (int i = 0; i < len; i++) {
        jlong rowid = elements[i];
        data.push_back(rowid);
    }
    pgm::PGMIndex<long, epsilon>* index = new pgm::PGMIndex<long, epsilon>(data);

    jclass segmentClass = (env)->FindClass("org/apache/cassandra/index/sai/disk/pgm/Segment");
    jclass listClass = (env)->FindClass("java/util/List");
    jfieldID interceptField = env->GetFieldID(segmentClass, "intercept", "I");
    jfieldID slopeField = env->GetFieldID(segmentClass, "slope", "F");
    jfieldID keyField = env->GetFieldID(segmentClass, "key", "J");
    jmethodID addMethod = env->GetMethodID(listClass, "add", "(Ljava/lang/Object;)Z");

    printf("segments.size=%d", index->segments.size());

    for (auto seg : index->segments) {
        jobject segment = env->AllocObject(segmentClass);

        env->SetIntField(segment, interceptField, seg.intercept);
        env->SetFloatField(segment, slopeField, seg.slope);
        env->SetLongField(segment, keyField, seg.key);

        env->CallObjectMethod(segmentList, addMethod, segment);
    }

    delete index;
}
