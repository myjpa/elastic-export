package repo.myjpa.elasticExport.util;

/**
 * report progress synchrously for a long-running task, used as call-back function
 * do not block this function
 * Created by haoliu on 7/7/2017.
 */
public interface ProgressReporter {
    /**
     * report current progress
     * @param offset current offset of subject(like documents)
     * @param size total count of subject(like documents)
     * @return true to continue, false to abort the task
     */
    boolean report(int offset, int size);
}
