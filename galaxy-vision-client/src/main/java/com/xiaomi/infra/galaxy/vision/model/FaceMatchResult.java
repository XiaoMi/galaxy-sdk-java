package com.xiaomi.infra.galaxy.vision.model;

public class FaceMatchResult {
    private boolean isMatch;
    private int score;
    public boolean getIsMatch() {
        return isMatch;
    }

    public void setIsMatch(boolean inMatch) {
        this.isMatch = inMatch;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

}
