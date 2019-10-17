db.getCollection("raw_data").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
				"charge_code": "CESTA001",
			}
		},

		// Stage 2
		{
			$group: {
				_id: "$channel",
				totalImpression: { $sum: "$impressions"}
			}
		},

		// Stage 3
		{
			$sort: {
				totalImpression: -1
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
