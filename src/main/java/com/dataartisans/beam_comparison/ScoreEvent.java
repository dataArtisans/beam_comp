package com.dataartisans.beam_comparison;

public class ScoreEvent {

	private Long userId;
	private Integer teamId;
	private Integer score;
	private Long timestamp;

	public ScoreEvent() {}

	public ScoreEvent(Long userId, Integer teamId, Integer score, Long timestamp) {
		this.userId = userId;
		this.teamId = teamId;
		this.score = score;
		this.timestamp = timestamp;
	}

	public Long getUserId() {
		return userId;
	}

	public Integer getTeamId() {
		return teamId;
	}

	public Integer getScore() {
		return score;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public void setTeamId(Integer teamId) {
		this.teamId = teamId;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String toString() {
		return	"User: " + this.userId +
				" Team: " + this.teamId +
				" Score: " + this.score +
				" Timestamp: " + this.timestamp;
	}
}
