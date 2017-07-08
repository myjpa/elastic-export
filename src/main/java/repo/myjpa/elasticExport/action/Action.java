/*
 *  MIT License
 *
 *  Copyright (c) 2017 Hao Liu (https://github.com/myjpa)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package repo.myjpa.elasticExport.action;

import repo.myjpa.elasticExport.util.OnProgress;

/**
 * an abstract Action
 * Created by haoliu on 7/7/2017.
 */
public abstract class Action {
    /**
     * create an action based on ad
     *
     * @param ad action descriptor
     * @return an action instance that is ready to run()
     */
    public static Action create(ActionDescriptor ad) {
        try {
            Class actionClass = Class.forName(ad.getActionClassName());
            Action action = (Action) actionClass.newInstance();
            action.setup(ad);
            return action;

        } catch (ClassNotFoundException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        } catch (InstantiationException e) {
            return null;
        }

    }

    protected ActionDescriptor ad;

    /**
     * validate and setup the action
     *
     * @param ad action descriptor
     * @throws IllegalArgumentException
     */
    protected void setup(ActionDescriptor ad) throws IllegalArgumentException {
        this.ad = ad;
    }

    /**
     * run this action with optionally progress reports
     *
     * @param onProgress call back to report progress, can be null
     * @throws Exception
     */
    public abstract void run(OnProgress onProgress) throws Exception;

    protected void validate(boolean condition, String description) throws IllegalArgumentException {
        if (!condition) throw new IllegalArgumentException(description);
    }

}
